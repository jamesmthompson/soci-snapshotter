/*
   Copyright The Soci Snapshotter Authors.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package metadata

import (
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"

	"github.com/awslabs/soci-snapshotter/ztoc"
	"github.com/awslabs/soci-snapshotter/ztoc/compression"
)

// memNode stores attributes for a single filesystem node.
type memNode struct {
	attr Attr
}

// memMeta stores metadata (children, tar info) for a node.
type memMeta struct {
	children           map[string]uint32 // base name -> child node ID
	tarName            string
	uncompressedOffset compression.Offset
	tarHeaderOffset    compression.Offset
	tarHeaderSize      compression.Offset
}

// memReader is an in-memory implementation of the Reader interface.
// All maps are populated during init and are read-only thereafter,
// making concurrent reads safe without mutexes.
type memReader struct {
	nodes  map[uint32]*memNode
	meta   map[uint32]*memMeta
	rootID uint32
	sr     *io.SectionReader
}

// NewMemReader creates an in-memory metadata Reader from a TOC.
func NewMemReader(sr *io.SectionReader, toc ztoc.TOC, opts ...Option) (Reader, error) {
	r := &memReader{
		nodes: make(map[uint32]*memNode),
		meta:  make(map[uint32]*memMeta),
		sr:    sr,
	}

	if err := r.init(toc); err != nil {
		return nil, err
	}
	return r, nil
}

func (r *memReader) init(toc ztoc.TOC) error {
	// Assign root
	r.rootID = 1
	r.nodes[r.rootID] = &memNode{attr: Attr{Mode: os.ModeDir | 0755, NumLink: 2}}
	r.meta[r.rootID] = &memMeta{children: make(map[string]uint32)}

	var nextID uint32 = 1 // root already used 1

	allocID := func() (uint32, error) {
		if nextID == math.MaxUint32 {
			return 0, fmt.Errorf("sequence id overflow")
		}
		nextID++
		return nextID, nil
	}

	// getOrCreateDir finds or creates directory nodes along the path.
	var getOrCreateDir func(dir string) (uint32, error)
	getOrCreateDir = func(dir string) (uint32, error) {
		if dir == "" || dir == "." {
			return r.rootID, nil
		}
		// Check if already exists by walking from root
		id, err := r.getIDByPath(dir)
		if err == nil {
			return id, nil
		}
		// Create it
		newID, err := allocID()
		if err != nil {
			return 0, err
		}
		r.nodes[newID] = &memNode{attr: Attr{Mode: os.ModeDir | 0755, NumLink: 2}}
		r.meta[newID] = &memMeta{children: make(map[string]uint32)}

		// Ensure parent exists
		parentDir := filepath.Dir(dir)
		if parentDir == dir || parentDir == "." {
			parentDir = ""
		}
		parentID, err := getOrCreateDir(parentDir)
		if err != nil {
			return 0, err
		}
		// Add to parent's children
		base := filepath.Base(dir)
		if r.meta[parentID].children == nil {
			r.meta[parentID].children = make(map[string]uint32)
		}
		r.meta[parentID].children[base] = newID
		// Increment parent numlink for directory child
		r.nodes[parentID].attr.NumLink++
		return newID, nil
	}

	for _, ent := range toc.FileMetadata {
		cleanName := cleanEntryPathMem(ent.Name)
		cleanLinkName := cleanEntryPathMem(ent.Linkname)

		isLink := ent.Type == "hardlink"
		isDir := ent.Type == "dir"

		var id uint32

		if isLink {
			// Find the link target
			targetID, err := r.getIDByPath(cleanLinkName)
			if err != nil {
				return fmt.Errorf("hardlink target %q not found: %w", cleanLinkName, err)
			}
			id = targetID
			r.nodes[id].attr.NumLink++
		} else {
			if isDir {
				// Check if dir already exists
				existingID, err := r.getIDByPath(cleanName)
				if err == nil {
					id = existingID
					// Overwrite attrs but preserve NumLink
					numLink := r.nodes[id].attr.NumLink
					r.nodes[id].attr = *attrFromZtocEntryMem(&ent, numLink)
				}
			}
			if id == 0 {
				var err error
				id, err = allocID()
				if err != nil {
					return err
				}
				numLink := 1
				if isDir {
					numLink = 2
				}
				r.nodes[id] = &memNode{attr: *attrFromZtocEntryMem(&ent, numLink)}
				if r.meta[id] == nil {
					r.meta[id] = &memMeta{}
				}
				if isDir {
					r.meta[id].children = make(map[string]uint32)
				}
			}
		}

		// Ensure parent directory exists and add this entry as child
		parentDirPath := filepath.Dir(cleanName)
		if parentDirPath == "." {
			parentDirPath = ""
		}
		parentID, err := getOrCreateDir(parentDirPath)
		if err != nil {
			return fmt.Errorf("failed to create parent dir for %q: %w", cleanName, err)
		}
		base := filepath.Base(cleanName)
		if r.meta[parentID].children == nil {
			r.meta[parentID].children = make(map[string]uint32)
		}
		r.meta[parentID].children[base] = id
		if isDir {
			r.nodes[parentID].attr.NumLink++
		}

		// Store metadata for non-link entries
		if !isLink {
			if r.meta[id] == nil {
				r.meta[id] = &memMeta{}
			}
			r.meta[id].tarName = ent.Name
			r.meta[id].uncompressedOffset = ent.UncompressedOffset
			r.meta[id].tarHeaderOffset = ent.TarHeaderOffset
			r.meta[id].tarHeaderSize = ent.UncompressedOffset - ent.TarHeaderOffset
		}
	}
	return nil
}

// getIDByPath walks the tree from root to find a node by path.
func (r *memReader) getIDByPath(path string) (uint32, error) {
	if path == "" || path == "." {
		return r.rootID, nil
	}
	parts := strings.Split(path, string(os.PathSeparator))
	current := r.rootID
	for _, part := range parts {
		if part == "" {
			continue
		}
		m := r.meta[current]
		if m == nil || m.children == nil {
			return 0, fmt.Errorf("path %q not found", path)
		}
		childID, ok := m.children[part]
		if !ok {
			return 0, fmt.Errorf("path %q not found at %q", path, part)
		}
		current = childID
	}
	return current, nil
}

func (r *memReader) RootID() uint32 {
	return r.rootID
}

func (r *memReader) GetAttr(id uint32) (Attr, error) {
	n, ok := r.nodes[id]
	if !ok {
		return Attr{}, fmt.Errorf("node %d not found", id)
	}
	return n.attr, nil
}

func (r *memReader) GetChild(pid uint32, base string) (uint32, Attr, error) {
	m := r.meta[pid]
	if m == nil || m.children == nil {
		return 0, Attr{}, fmt.Errorf("no children for %d", pid)
	}
	childID, ok := m.children[base]
	if !ok {
		return 0, Attr{}, fmt.Errorf("child %q not found in %d", base, pid)
	}
	n, ok := r.nodes[childID]
	if !ok {
		return 0, Attr{}, fmt.Errorf("child node %d not found", childID)
	}
	return childID, n.attr, nil
}

func (r *memReader) ForeachChild(id uint32, f func(name string, id uint32, mode os.FileMode) bool) error {
	m := r.meta[id]
	if m == nil || m.children == nil {
		return nil
	}
	for name, childID := range m.children {
		n := r.nodes[childID]
		if n == nil {
			continue
		}
		if !f(name, childID, n.attr.Mode) {
			break
		}
	}
	return nil
}

func (r *memReader) OpenFile(id uint32) (File, error) {
	n, ok := r.nodes[id]
	if !ok {
		return nil, fmt.Errorf("node %d not found", id)
	}
	if !n.attr.Mode.IsRegular() {
		return nil, fmt.Errorf("node %d is not a regular file", id)
	}
	m := r.meta[id]
	if m == nil {
		return nil, fmt.Errorf("metadata for %d not found", id)
	}
	return &file{
		tarName:            m.tarName,
		uncompressedOffset: m.uncompressedOffset,
		uncompressedSize:   compression.Offset(n.attr.Size),
		tarHeaderOffset:    m.tarHeaderOffset,
		tarHeaderSize:      m.tarHeaderSize,
	}, nil
}

func (r *memReader) Clone(sr *io.SectionReader) (Reader, error) {
	return &memReader{
		nodes:  r.nodes,
		meta:   r.meta,
		rootID: r.rootID,
		sr:     sr,
	}, nil
}

func (r *memReader) Close() error {
	return nil
}

func cleanEntryPathMem(path string) string {
	return strings.TrimPrefix(filepath.Clean(string(os.PathSeparator)+path), string(os.PathSeparator))
}

func attrFromZtocEntryMem(src *ztoc.FileMetadata, numLink int) *Attr {
	xattrs := make(map[string][]byte)
	for k, v := range src.Xattrs() {
		xattrs[k] = []byte(v)
	}
	return &Attr{
		Size:     int64(src.UncompressedSize),
		ModTime:  src.ModTime,
		LinkName: src.Linkname,
		Mode:     src.FileMode(),
		UID:      src.UID,
		GID:      src.GID,
		DevMajor: int(src.Devmajor),
		DevMinor: int(src.Devminor),
		Xattrs:   xattrs,
		NumLink:  numLink,
	}
}
