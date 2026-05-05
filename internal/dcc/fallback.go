package dcc

import (
	"big-market-kratos/internal/conf"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/go-kratos/kratos/v2/log"
)

// Fallback 负责本地磁盘快照的读写，提供降级容灾能力
type Fallback struct {
	filePath string
	mu       sync.Mutex
	log      *log.Helper
}

func NewFallback(fallbackPath string, logger log.Logger) *Fallback {
	return &Fallback{
		filePath: fallbackPath,
		log:      log.NewHelper(log.With(logger, "module", "dcc-fallback")),
	}
}

// SaveSnapshot 将最新的配置快照安全地保存到本地磁盘
func (f *Fallback) SaveSnapshot(cfg *conf.Dcc) {
	// 加锁，防止短时间内 etcd 频繁变更导致多个协程并发写文件
	f.mu.Lock()
	defer f.mu.Unlock()

	// 1. 确保目录存在
	dir := filepath.Dir(f.filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		f.log.Errorf("❌ 创建 Fallback 目录失败: %v", err)
		return
	}

	// 2. 将配置序列化为带缩进的 JSON (方便运维人员紧急手动修改)
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		f.log.Errorf("❌ 序列化 Fallback 快照失败: %v", err)
		return
	}

	// 原子写文件
	tmpFile := f.filePath + ".tmp"
	if err := os.WriteFile(tmpFile, data, 0644); err != nil {
		f.log.Errorf("❌ 写入临时快照文件失败: %v", err)
		return
	}

	if err := os.Rename(tmpFile, f.filePath); err != nil {
		f.log.Errorf("❌ 替换正式快照文件失败: %v", err)
		return
	}

	f.log.Debugf("💾 本地配置快照已更新: %s", f.filePath)
}

// LoadSnapshot 在 etcd 连接失败时，从本地磁盘读取兜底配置
func (f *Fallback) LoadSnapshot() (*conf.Dcc, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	// 1. 检查文件是否存在
	if _, err := os.Stat(f.filePath); os.IsNotExist(err) {
		return nil, fmt.Errorf("本地兜底文件不存在: %s", f.filePath)
	}

	// 2. 读取文件内容
	data, err := os.ReadFile(f.filePath)
	if err != nil {
		return nil, fmt.Errorf("读取兜底文件失败: %v", err)
	}

	// 3. 反序列化到结构体
	var cfg conf.Dcc
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("解析兜底文件 JSON 失败 (文件可能已损坏): %v", err)
	}

	return &cfg, nil
}
