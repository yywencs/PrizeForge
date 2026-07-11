package dbutil

import (
	"errors"

	"gorm.io/gorm"
)

func IgnoreNotFound(err error) error {
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil
	}
	return err
}

// IsNotFound 简单判断是否是未找到
func IsNotFound(err error) bool {
	return errors.Is(err, gorm.ErrRecordNotFound)
}
