# Filesystem Storage Backend

The filesystem backend provides support for the local filesystem.

## Configuration

To use the filesystem backend, you need to specify the following option in your connection string:

- `workingDir`: The directory path on your filesystem.

A connection string for the filesystem backend looks like this:

```
fs:///path/to/dir
```

Replace `/path/to/dir` with the actual path to your working directory.