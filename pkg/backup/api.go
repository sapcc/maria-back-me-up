package backup

import (
	"fmt"
	"os"
	"time"

	"github.com/sapcc/maria-back-me-up/pkg/config"
)

// CreateIncBackup creates inc backups
func (m *Manager) CreateIncBackup() (err error) {
	if m.cronBackup == nil {
		return fmt.Errorf("no backup running")
	}
	e := m.cronBackup.Entries()
	if len(e) == 1 {
		t := e[0].Next
		d := t.Sub(time.Now())
		if d.Minutes() > 1 {
			return m.Db.FlushIncBackup()
		}
		return fmt.Errorf("can't create inc backup as full backup is about to be scheduled")
	}
	return fmt.Errorf("no backup running")
}

// GetConfig returns the config
func (m *Manager) GetConfig() config.Config {
	return m.cfg
}

// GetBackupActive returns if a backup is currently active
func (m *Manager) GetBackupActive() bool {
	if m.cronBackup == nil {
		return false
	}
	return true
}

// GetHealthStatus returns the current backup health status
func (m *Manager) GetHealthStatus() *UpdateStatus {
	m.updateSts.RLock()
	defer m.updateSts.RUnlock()
	return m.updateSts
}

// Restore triggers a restore
func (m *Manager) Restore(p string) (err error) {
	logger.Info("starting restore")
	m.Health.Lock()
	m.Health.Ready = false
	m.Health.Unlock()
	logger.Debug("restore with sidecar: ", *m.cfg.SideCar)
	defer func() {
		if m.cfg.SideCar != nil && !*m.cfg.SideCar {
			ip, err := m.k8sDB.GetPodIP(fmt.Sprintf("app=%s-mariadb", m.cfg.ServiceName))
			if err != nil {
				logger.Error("Cannot set pod databse to ready")
			}
			if err = sendReadinessRequest([]byte(`{"ready":true}`), ip); err != nil {
				logger.Error("Cannot set pod databse to ready")
			}
		}
		m.Health.Lock()
		m.Health.Ready = true
		m.Health.Unlock()
	}()
	if m.cfg.SideCar != nil && !*m.cfg.SideCar {
		if err = sendReadinessRequest([]byte(`{"ready":false}`), m.cfg.Database.Host); err != nil {
			return
		}
	}

	if err = m.k8sDB.CheckPodNotReady(fmt.Sprintf("app=%s-mariadb", m.cfg.ServiceName)); err != nil {
		return fmt.Errorf("cannot set pod to status: NotReady. reason: %s", err.Error())
	}
	if err = m.Db.Restore(p); err != nil {
		return
	}
	logger.Info("restore successful")
	//only remove backup files when restore was succesful, so manual restore is possible!
	os.RemoveAll(p)

	return
}
