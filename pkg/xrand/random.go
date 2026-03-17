package xrand

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

func RandomNumeric(n int) string {
	b := make([]byte, n)
	for i := range b {
		num, _ := rand.Int(rand.Reader, big.NewInt(int64(10)))
		b[i] = byte('0' + num.Int64())
	}
	return string(b)
}
