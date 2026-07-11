package utils

import (
	"crypto/rand"
	"math/big"
)

func GetSecureRandomInt(max int) (int, error) {
	// crypto/rand 生成的是 *big.Int
	n, err := rand.Int(rand.Reader, big.NewInt(int64(max)))
	if err != nil {
		return 0, err
	}
	return int(n.Int64()), nil
}
