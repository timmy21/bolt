package bolt

import (
	"syscall"
)

// 在Linux中，为了性能考虑，write/pwrite等系统调用不会等待设备I/O完成后再返回。
// write/pwrite等系统调用只会更新page cache，而脏页的同步时间由操作系统控制。
// sync系统调用会在page cache中的脏页提交到设备I/O队列后返回，但是不会等待设备I/O完成。如果此时I/O设备故障，则数据还可能丢失。
// 而fsync与fdatasync则会等待设备I/O完成后返回，以提供最高的同步保证。
//
// fdatasync() 会将文件对应的 dirty page 写回 disk，从而保证机器挂掉时数据是完好的，
// fsync() 除了 dirty page 外，还会把文件 metadata(inode) 写回 disk
// 当然 disk 可能也有 disk cache，但这是操作系统能做到的最大保证了
//
// fdatasync flushes written data to a file descriptor.
func fdatasync(db *DB) error {
	return syscall.Fdatasync(int(db.file.Fd()))
}
