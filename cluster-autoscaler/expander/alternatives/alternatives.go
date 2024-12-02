package alternatives

import (
	"fmt"

	"gopkg.in/yaml.v2"

	"k8s.io/autoscaler/cluster-autoscaler/expander"

	apiv1 "k8s.io/api/core/v1"
	v1lister "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/record"
	klog "k8s.io/klog/v2"
)

const (
	// AlternativesConfigMapName defines a name of the ConfigMap used to store alternatives configuration
	AlternativesConfigMapName = "cluster-autoscaler-alternative-selector"
	// AlternativesConfigMapKey defines the key used in the ConfigMap to configure alternatives
	AlternativesConfigMapKey = "alternatives"
)

type alternatives map[string][]string

type alternativeSelector struct {
	logRecorder      record.EventRecorder
	okConfigUpdates  int
	badConfigUpdates int
	configMapLister  v1lister.ConfigMapNamespaceLister
}

// NewSelector returns an alternative selector that picks node groups based on user-defined alternatives.
func NewSelector(configMapLister v1lister.ConfigMapNamespaceLister,
	logRecorder record.EventRecorder) expander.AlternativeSelector {
	res := &alternativeSelector{
		logRecorder:     logRecorder,
		configMapLister: configMapLister,
	}
	return res
}

func (p *alternativeSelector) reloadConfigMap() (alternatives, *apiv1.ConfigMap, error) {
	cm, err := p.configMapLister.Get(AlternativesConfigMapName)
	if err != nil {
		return nil, nil, fmt.Errorf("Alternatives config map %s not found: %v", AlternativesConfigMapName, err)
	}

	altString, found := cm.Data[AlternativesConfigMapKey]
	if !found {
		return nil, cm, nil
	}

	newAlternatives, err := p.parseAlternativesYAMLString(altString)
	if err != nil {
		msg := fmt.Sprintf("Wrong configuration for alternatives in alternatives selector: %v. Ignoring update.", err)
		p.logConfigWarning(cm, "AlternativesConfigMapInvalid", msg)
		return nil, cm, err
	}

	return newAlternatives, cm, nil
}

func (p *alternativeSelector) logConfigWarning(cm *apiv1.ConfigMap, reason, msg string) {
	p.logRecorder.Event(cm, apiv1.EventTypeWarning, reason, msg)
	klog.Warning(msg)
	p.badConfigUpdates++
}

func (p *alternativeSelector) parseAlternativesYAMLString(alternativesYAML string) (alternatives, error) {
	if alternativesYAML == "" {
		return nil, fmt.Errorf("alternatives configuration in %s configmap is empty; please provide valid configuration",
			AlternativesConfigMapName)
	}
	var newAlternatives map[string][]string
	if err := yaml.Unmarshal([]byte(alternativesYAML), &newAlternatives); err != nil {
		return nil, fmt.Errorf("Can't parse YAML with alternatives in the configmap: %v", err)
	}

	p.okConfigUpdates++
	msg := "Successfully loaded alternatives configuration from configmap."
	klog.V(4).Info(msg)

	return newAlternatives, nil
}

func (as *alternativeSelector) GetAlternativeOptions(options []expander.Option, best *expander.Option) []expander.Option {
	alternatives, _, err := as.reloadConfigMap()
	if err != nil {
		return nil
	}

	if best == nil {
		return nil
	}

	alternative, found := alternatives[best.NodeGroup.Id()]
	if !found {
		return nil
	}

	var opts []expander.Option
	for _, alt := range alternative {
		for _, opt := range options {
			if opt.NodeGroup.Id() == alt {
				opts = append(opts, opt)
				break
			}
		}
	}

	return opts
}

func (as *alternativeSelector) GetAlternativeNodeGroups(nodeGroup string) []string {
	alternatives, _, err := as.reloadConfigMap()
	if err != nil {
		return nil
	}

	return alternatives[nodeGroup]
}
