package capacity

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

type WatcherConfig struct {
	ConfigMapName string
	Namespace     string
	PollInterval  time.Duration
	Logger        *slog.Logger
	OnChange      func(maxRunners int)
}

type ConfigMapWatcher struct {
	clientset *kubernetes.Clientset
	config    WatcherConfig
	lastValue int
}

func NewConfigMapWatcher(config WatcherConfig) (*ConfigMapWatcher, error) {
	if config.PollInterval == 0 {
		config.PollInterval = 5 * time.Second
	}
	if config.Logger == nil {
		config.Logger = slog.New(slog.DiscardHandler)
	}

	restConfig, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to get in-cluster config: %w", err)
	}

	clientset, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create kubernetes clientset: %w", err)
	}

	return &ConfigMapWatcher{
		clientset: clientset,
		config:    config,
		lastValue: -1,
	}, nil
}

func (w *ConfigMapWatcher) Run(ctx context.Context) error {
	ticker := time.NewTicker(w.config.PollInterval)
	defer ticker.Stop()

	w.config.Logger.Info("Starting ConfigMap watcher",
		"configMap", w.config.ConfigMapName,
		"namespace", w.config.Namespace,
		"pollInterval", w.config.PollInterval,
	)

	w.poll(ctx)

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			w.poll(ctx)
		}
	}
}

func (w *ConfigMapWatcher) poll(ctx context.Context) {
	cm, err := w.clientset.CoreV1().ConfigMaps(w.config.Namespace).Get(
		ctx, w.config.ConfigMapName, metav1.GetOptions{},
	)
	if err != nil {
		w.config.Logger.Debug("ConfigMap not available", "configMap", w.config.ConfigMapName, "error", err)
		return
	}

	val, ok := cm.Data["maxRunners"]
	if !ok {
		w.config.Logger.Debug("ConfigMap missing maxRunners key", "configMap", w.config.ConfigMapName)
		return
	}

	maxRunners, err := strconv.Atoi(strings.TrimSpace(val))
	if err != nil {
		w.config.Logger.Warn("Invalid maxRunners value in ConfigMap", "value", val, "error", err)
		return
	}

	if maxRunners < 0 {
		w.config.Logger.Warn("Negative maxRunners value in ConfigMap, ignoring", "value", maxRunners)
		return
	}

	if maxRunners == w.lastValue {
		return
	}

	w.config.Logger.Info("maxRunners changed via ConfigMap",
		"old", w.lastValue,
		"new", maxRunners,
		"configMap", w.config.ConfigMapName,
	)
	w.lastValue = maxRunners
	w.config.OnChange(maxRunners)
}
