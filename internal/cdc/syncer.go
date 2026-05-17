package cdc

import (
	"fmt"
	"time"

	"github.com/go-mysql-org/go-mysql/canal"
)

type Syncer struct {
	canal *canal.Canal
}

func NewSyncer(cfg *Config, handler canal.EventHandler) (*Syncer, error) {
	readTimeout, err := time.ParseDuration(cfg.MySQLReadTimeout)
	if err != nil {
		return nil, fmt.Errorf("parse CDC_MYSQL_READ_TIMEOUT: %w", err)
	}

	canalCfg := canal.NewDefaultConfig()
	canalCfg.Addr = cfg.MySQLAddr
	canalCfg.User = cfg.MySQLUser
	canalCfg.Password = cfg.MySQLPassword
	canalCfg.Charset = cfg.MySQLCharset
	canalCfg.Flavor = cfg.MySQLFlavor
	canalCfg.ServerID = cfg.MySQLServerID
	canalCfg.ReadTimeout = readTimeout
	canalCfg.ParseTime = true
	canalCfg.IncludeTableRegex = cfg.IncludeTableRegex
	canalCfg.Dump.ExecutionPath = ""

	instance, err := canal.NewCanal(canalCfg)
	if err != nil {
		return nil, fmt.Errorf("new canal: %w", err)
	}

	instance.SetEventHandler(handler)

	return &Syncer{canal: instance}, nil
}

func (s *Syncer) Run() error {
	return s.canal.Run()
}

func (s *Syncer) Close() {
	if s == nil || s.canal == nil {
		return
	}
	s.canal.Close()
}
