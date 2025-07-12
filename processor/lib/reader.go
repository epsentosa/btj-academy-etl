package lib

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
)

func ExtractTarGz(data []byte) (map[string][]byte, error) {
	files := make(map[string][]byte)

	gzipReader, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("failed to create gzip reader: %w", err)
	}
	defer gzipReader.Close()

	tarReader := tar.NewReader(gzipReader)

	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to read tar header: %w", err)
		}

		if header.Typeflag == tar.TypeDir {
			continue
		}

		var fileContent bytes.Buffer
		_, err = io.Copy(&fileContent, tarReader)
		if err != nil {
			return nil, fmt.Errorf("failed to read file content: %w", err)
		}

		files[header.Name] = fileContent.Bytes()
	}

	return files, nil
}
