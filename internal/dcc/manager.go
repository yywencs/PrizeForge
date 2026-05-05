package dcc

import (
	"big-market-kratos/internal/conf"
	"fmt"
	"sync/atomic"

	"github.com/go-kratos/kratos/v2/config"
	"github.com/go-kratos/kratos/v2/log"
)

type ConfigGetter interface {
	Get() *conf.Dcc
}

var _ ConfigGetter = (*Manager)(nil)

type Manager struct {
	source   config.Config
	value    atomic.Value
	watchKey string      // 需要监听的 key（比如 "dcc"）
	fallback *Fallback   // 本地磁盘快照备份组件
	log      *log.Helper // Kratos 日志组件
}

func NewManager(c config.Config, watchKey string, logger log.Logger, fallback *Fallback) *Manager {
	return &Manager{
		source:   c,
		value:    atomic.Value{},
		watchKey: watchKey,
		fallback: fallback,
		log:      log.NewHelper(log.With(logger, "module", "dcc/manager")),
	}
}

func (m *Manager) Init() error {
	var cfg conf.Dcc

	// 1. 尝试从远程配置中心 (etcd) 拉取
	if err := m.source.Load(); err != nil {
		m.log.Warnf("🔥 无法连接远程配置中心，尝试读取本地快照: %v", err)

		fallbackCfg, fbErr := m.fallback.LoadSnapshot()
		if fbErr != nil {
			return fmt.Errorf("致命错误: 远程配置中心挂掉，且本地快照读取失败: %v", fbErr)
		}

		cfg = *fallbackCfg

		m.log.Info("✅ 成功加载本地配置快照")
		m.value.Store(&cfg)
		return nil
	}

	// 2. 远程拉取成功，扫描到新结构体
	if err := m.source.Value(m.watchKey).Scan(&cfg); err != nil {
		return fmt.Errorf("解析远程配置失败: %v", err)
	}

	// 3. 首次加载成功：更新内存 Cache + 异步备份到本地文件
	m.value.Store(&cfg)
	go m.fallback.SaveSnapshot(&cfg)

	// 4. 开启后台 Watch 监听
	go m.watch()

	m.log.Info("✅ DCC 配置中心初始化完成")
	return nil
}

func (m *Manager) watch() {
	err := m.source.Watch(m.watchKey, func(key string, value config.Value) {
		m.log.Infof("🚀 检测到配置变更: [%s]", key)

		var newCfg conf.Dcc
		if err := value.Scan(&newCfg); err != nil {
			m.log.Errorf("❌ 变更配置解析失败，放弃此次热更新: %v", err)
			return
		}

		// 1. 原子替换内存快照 (无锁)
		m.value.Store(&newCfg)

		// 2. 异步更新本地磁盘备份
		go m.fallback.SaveSnapshot(&newCfg)

		m.log.Info("✅ 配置热更新与本地备份完成")
	})

	if err != nil {
		m.log.Errorf("🔥 Watch 监听异常退出: %v", err)
	}
}

func (m *Manager) Get() *conf.Dcc {
	v := m.value.Load()
	if v == nil {
		return &conf.Dcc{}
	}
	return v.(*conf.Dcc)
}
