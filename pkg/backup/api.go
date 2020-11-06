package backup

import (
	"fmt"
	"os"
	"time"

	"github.com/sapcc/maria-back-me-up/pkg/config"
)

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

func (m *Manager) GetConfig() config.Config {
	return m.cfg
}

func (m *Manager) GetBackupActive() bool {
	if m.cronBackup == nil {
		return false
	}
	return true
}

func (m *Manager) GetHealthStatus() *updateStatus {
	m.updateSts.RLock()
	defer m.updateSts.RUnlock()
	return m.updateSts
}

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
