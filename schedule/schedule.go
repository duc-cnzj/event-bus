package schedule

import (
	dlm "github.com/DuC-cnZj/dlm"
	"github.com/robfig/cron/v3"
	log "github.com/sirupsen/logrus"
	"mq/adapter"
	"mq/bootstrapers"
	"mq/hub"
	"sync"
)

type Interface interface {
	Run()
	Stop()
}

var app = bootstrapers.App()

type CronJob struct {
	Spec    string
	Cmd     func(hub.Interface, *sync.Map) func()
	Enabled bool
}

func cronJobs() []CronJob {
	return []CronJob{
		{Spec: "@every 1s", Cmd: Republish, Enabled: app.Config().CronRepublishEnabled},
		{Spec: "@every 1s", Cmd: DelayPublish, Enabled: app.Config().CronDelayPublishEnabled},
	}
}

type Schedule struct {
	lockList *sync.Map
	cron     *cron.Cron
	hub      hub.Interface
}

func NewSchedule(hub hub.Interface) Interface {
	return &Schedule{
		hub:      hub,
		lockList: &sync.Map{},
		cron: cron.New(cron.WithChain(
			cron.Recover(&adapter.CronLoggerAdapter{}),
		)),
	}
}

func (s *Schedule) Run() {
	num := 0
	for _, job := range cronJobs() {
		if job.Enabled {
			num++
			s.cron.AddFunc(job.Spec, job.Cmd(s.hub, s.lockList))
		}
	}

	s.cron.Start()
	log.Infof("定时任务开始执行, 一共 %d 个定时任务", num)
}

func (s *Schedule) Stop() {
	s.cron.Stop()

	s.lockList.Range(func(key, value interface{}) bool {
		l := value.(*dlm.Lock)
		log.Debug("Release: ", l.GetCurrentOwner())
		l.ForceRelease()
		return true
	})

	log.Warnf("定时任务停止！")
}
