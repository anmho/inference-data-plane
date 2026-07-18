package catalog

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"

	controlv1 "github.com/anmho/inference-data-plane/controlplane/gen/inference/control/v1"
	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"
)

const annotationPrefix = "inference.anmho.com/"

type document struct {
	Kind     string `yaml:"kind"`
	Metadata struct {
		Annotations map[string]string `yaml:"annotations"`
	} `yaml:"metadata"`
	Spec struct {
		Model struct {
			Name string `yaml:"name"`
		} `yaml:"model"`
	} `yaml:"spec"`
}

type Entry struct {
	Model         *controlv1.Model
	Endpoint      string
	KServeService string
}

type Catalog struct {
	mu      sync.RWMutex
	pattern string
	models  map[string]Entry
}

func Load(pattern string) (*Catalog, error) {
	catalog := &Catalog{pattern: pattern}
	if err := catalog.Reload(); err != nil {
		return nil, err
	}
	return catalog, nil
}

func (c *Catalog) Reload() error {
	paths, err := filepath.Glob(c.pattern)
	if err != nil {
		return fmt.Errorf("expand model catalog glob: %w", err)
	}
	models := make(map[string]Entry)
	for _, path := range paths {
		file, err := os.Open(path)
		if err != nil {
			return fmt.Errorf("open model declaration %s: %w", path, err)
		}
		decoder := yaml.NewDecoder(file)
		for {
			var doc document
			err = decoder.Decode(&doc)
			if err == io.EOF {
				break
			}
			if err != nil {
				file.Close()
				return fmt.Errorf("decode model declaration %s: %w", path, err)
			}
			if doc.Kind != "LLMInferenceServiceConfig" || doc.Spec.Model.Name == "" {
				continue
			}
			a := doc.Metadata.Annotations
			contextLimit, err := parseUint(a[annotationPrefix+"context-limit"])
			if err != nil {
				return fmt.Errorf("%s context limit: %w", path, err)
			}
			maxOutput, err := parseUint(a[annotationPrefix+"max-output-tokens"])
			if err != nil {
				return fmt.Errorf("%s max output tokens: %w", path, err)
			}
			models[doc.Spec.Model.Name] = Entry{
				Model: &controlv1.Model{
					Id:                doc.Spec.Model.Name,
					Revision:          a[annotationPrefix+"revision"],
					TokenizerId:       a[annotationPrefix+"tokenizer-id"],
					TokenizerRevision: a[annotationPrefix+"tokenizer-revision"],
					ContextLimit:      contextLimit,
					MaxOutputTokens:   maxOutput,
					LocalRuntime:      a[annotationPrefix+"local-runtime"],
					CloudRuntime:      a[annotationPrefix+"cloud-runtime"],
				},
				Endpoint:      a[annotationPrefix+"endpoint"],
				KServeService: a[annotationPrefix+"kserve-service"],
			}
		}
		file.Close()
	}
	if len(models) == 0 {
		return fmt.Errorf("no LLMInferenceServiceConfig declarations matched %s", c.pattern)
	}
	c.mu.Lock()
	c.models = models
	c.mu.Unlock()
	return nil
}

func (c *Catalog) Get(id string) (Entry, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	entry, ok := c.models[id]
	if !ok {
		return Entry{}, false
	}
	entry.Model = cloneModel(entry.Model)
	return entry, true
}

func (c *Catalog) List() []*controlv1.Model {
	c.mu.RLock()
	defer c.mu.RUnlock()
	ids := make([]string, 0, len(c.models))
	for id := range c.models {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	models := make([]*controlv1.Model, 0, len(ids))
	for _, id := range ids {
		models = append(models, cloneModel(c.models[id].Model))
	}
	return models
}

func parseUint(value string) (uint64, error) {
	if value == "" {
		return 0, nil
	}
	return strconv.ParseUint(value, 10, 64)
}

func cloneModel(model *controlv1.Model) *controlv1.Model {
	return proto.Clone(model).(*controlv1.Model)
}
