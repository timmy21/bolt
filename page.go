package bolt

import (
	"fmt"
	"os"
	"sort"
	"unsafe"
)

const pageHeaderSize = int(unsafe.Offsetof(((*page)(nil)).ptr))

const minKeysPerPage = 2

const branchPageElementSize = int(unsafe.Sizeof(branchPageElement{}))
const leafPageElementSize = int(unsafe.Sizeof(leafPageElement{}))

const (
	branchPageFlag   = 0x01
	leafPageFlag     = 0x02
	metaPageFlag     = 0x04
	freelistPageFlag = 0x10
)

const (
	bucketLeafFlag = 0x01
)

type pgid uint64

type page struct {
	id pgid
	// page类型标志。分为：metaPageFlag、freelistPageFlag、branchPageFlag、leafPageFlag
	flags uint16
	// 记录 page 中的元素个数
	count uint16
	// 当遇到体积巨大、单个 page 无法装下的数据时，会溢出到其它 pages，overflow 记录溢出总数
	// 数据溢出当前page，用overflow表示后续的N个page都是当前page的数据。overflow的后续page只存放data，不存header
	overflow uint32
	// 指向page数据起始位置的指针
	ptr uintptr
}

// typ returns a human readable page type string used for debugging.
func (p *page) typ() string {
	if (p.flags & branchPageFlag) != 0 {
		return "branch"
	} else if (p.flags & leafPageFlag) != 0 {
		return "leaf"
	} else if (p.flags & metaPageFlag) != 0 {
		return "meta"
	} else if (p.flags & freelistPageFlag) != 0 {
		return "freelist"
	}
	return fmt.Sprintf("unknown<%02x>", p.flags)
}

// meta returns a pointer to the metadata section of the page.
func (p *page) meta() *meta {
	return (*meta)(unsafe.Pointer(&p.ptr))
}

// leafPageElement retrieves the leaf node by index
func (p *page) leafPageElement(index uint16) *leafPageElement {
	n := &((*[0x7FFFFFF]leafPageElement)(unsafe.Pointer(&p.ptr)))[index]
	return n
}

// leafPageElements retrieves a list of leaf nodes.
func (p *page) leafPageElements() []leafPageElement {
	if p.count == 0 {
		return nil
	}
	return ((*[0x7FFFFFF]leafPageElement)(unsafe.Pointer(&p.ptr)))[:]
}

// branchPageElement retrieves the branch node by index
func (p *page) branchPageElement(index uint16) *branchPageElement {
	return &((*[0x7FFFFFF]branchPageElement)(unsafe.Pointer(&p.ptr)))[index]
}

// branchPageElements retrieves a list of branch nodes.
func (p *page) branchPageElements() []branchPageElement {
	if p.count == 0 {
		return nil
	}
	return ((*[0x7FFFFFF]branchPageElement)(unsafe.Pointer(&p.ptr)))[:]
}

// dump writes n bytes of the page to STDERR as hex output.
func (p *page) hexdump(n int) {
	buf := (*[maxAllocSize]byte)(unsafe.Pointer(p))[:n]
	fmt.Fprintf(os.Stderr, "%x\n", buf)
}

type pages []*page

func (s pages) Len() int           { return len(s) }
func (s pages) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s pages) Less(i, j int) bool { return s[i].id < s[j].id }

// 在 page header 之后，顺序地排列着 page 中每个键(值)数据的元信息，即为 element header，
// 每个 element header 记录具体键(值)数据的位置和大小，这种布局技术也被称为 Slotted Pages。

// 非叶子节点的存储方式
// ptr(branch): branchPageElement1 | branchPageElement2 | ... | branchPageElementN | k1 | k2 | ... | kn
// branchPageElement: pos | ksize | pgid

// branchPageElement represents a node on a branch page.
type branchPageElement struct {
	pos   uint32 //该元信息和真实key之间的偏移量
	ksize uint32
	pgid  pgid //子节点所在 page 的 id
}

// key returns a byte slice of the node key.
func (n *branchPageElement) key() []byte {
	buf := (*[maxAllocSize]byte)(unsafe.Pointer(n))
	return (*[maxAllocSize]byte)(unsafe.Pointer(&buf[n.pos]))[:n.ksize]
}

// 数据溢出，当需要存储超过单个 page 大小的数据时，比如：
//
// 1. branch/leaf page 遇到大的键值对数据
// 2. freelist page 中空闲 page 列表较长
// 我们该如何处理？Bolt 通过引入 overflow page 来解决这个问题，
// 当需要存储超过单个 page 大小的数据时，就申请满足大小需求的连续 N 个 page，
// 其中第 2 个到第 N 个是第 1 个 page 的 overflow，它们与第 1 个 page 共用 page header：

// 叶子节点的存储方式
// ptr(leaf): leafPageElement1 | leafPageElement2 | ... | leafPageElementN | k1 | v1 | k2 | v2 | ... | kn | vn
// leafPageElement: flags | pos | ksize | vsize
// flags:
//    0: 叶子节点为普通的key-value类型
//    1: 叶子节点为桶类型，其key为桶的key，当桶中的元素很少时，value会填充为桶的pgid以及其内联的kv节点数据
//
// boltdb支持任意长度的key和value，因此无法直接结构化保存key和value的列表。
// 为了解决这一问题，branch page和leaf page的Page Body起始处是一个由定长的索引（branchPageElement或leafPageElement）组成的列表，
// 第i个索引记录了第i个key或key/value的起始位置与key的长度或key/value各自的长度

// leafPageElement represents a node on a leaf page.
type leafPageElement struct {
	//该值主要用来区分，是子桶叶子节点元素还是普通的key/value叶子节点元素。flags值为1时表示子桶。否则为key/value
	flags uint32
	// key值相对于此Element起始位置的偏移
	// &leafPageElement + pos == &key。
	// &leafPageElement + pos + ksize == &val
	pos   uint32
	ksize uint32
	vsize uint32
}

// key returns a byte slice of the node key.
func (n *leafPageElement) key() []byte {
	buf := (*[maxAllocSize]byte)(unsafe.Pointer(n))
	return (*[maxAllocSize]byte)(unsafe.Pointer(&buf[n.pos]))[:n.ksize:n.ksize]
}

// value returns a byte slice of the node value.
func (n *leafPageElement) value() []byte {
	buf := (*[maxAllocSize]byte)(unsafe.Pointer(n))
	return (*[maxAllocSize]byte)(unsafe.Pointer(&buf[n.pos+n.ksize]))[:n.vsize:n.vsize]
}

// PageInfo represents human readable information about a page.
type PageInfo struct {
	ID            int
	Type          string
	Count         int
	OverflowCount int
}

type pgids []pgid

func (s pgids) Len() int           { return len(s) }
func (s pgids) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s pgids) Less(i, j int) bool { return s[i] < s[j] }

// merge returns the sorted union of a and b.
func (a pgids) merge(b pgids) pgids {
	// Return the opposite slice if one is nil.
	if len(a) == 0 {
		return b
	}
	if len(b) == 0 {
		return a
	}
	merged := make(pgids, len(a)+len(b))
	mergepgids(merged, a, b)
	return merged
}

// mergepgids copies the sorted union of a and b into dst.
// If dst is too small, it panics.
func mergepgids(dst, a, b pgids) {
	if len(dst) < len(a)+len(b) {
		panic(fmt.Errorf("mergepgids bad len %d < %d + %d", len(dst), len(a), len(b)))
	}
	// Copy in the opposite slice if one is nil.
	if len(a) == 0 {
		copy(dst, b)
		return
	}
	if len(b) == 0 {
		copy(dst, a)
		return
	}

	// Merged will hold all elements from both lists.
	merged := dst[:0]

	// Assign lead to the slice with a lower starting value, follow to the higher value.
	lead, follow := a, b
	if b[0] < a[0] {
		lead, follow = b, a
	}

	// Continue while there are elements in the lead.
	for len(lead) > 0 {
		// Merge largest prefix of lead that is ahead of follow[0].
		n := sort.Search(len(lead), func(i int) bool { return lead[i] > follow[0] })
		merged = append(merged, lead[:n]...)
		if n >= len(lead) {
			break
		}

		// Swap lead and follow.
		lead, follow = follow, lead[n:]
	}

	// Append what's left in follow.
	_ = append(merged, follow...)
}
