/* Copyright 2020 Joeri Hermans, Victor Penso, Matteo Dessalvi

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>. */

package main

import (
	"io/ioutil"
	"os/exec"
	"strconv"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/log"
)

func GPUsUsageData() []byte {
	cmd := exec.Command("squeue","-a","-r","-h","-o %j|%u|%R|%b")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		log.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		log.Fatal(err)
	}
	out, _ := ioutil.ReadAll(stdout)
	if err := cmd.Wait(); err != nil {
		log.Fatal(err)
	}
	return out
}

type GPUsUsageMetrics struct {
	alloc       float64
}

func ParseAllocatedGPUsUsage(input []byte) map[string]*GPUsUsageMetrics {
	gpu_usages := make(map[string]*GPUsUsageMetrics)
	lines := strings.Split(string(input), "\n")
	for _, line := range lines {
		if strings.Contains(line,"|") {
				user := strings.Split(line,"|")[1]
				node := strings.Split(line,"|")[2]
				splicer := user + ":" + node
				allocStr := strings.Split(strings.Split(strings.Split(line,"|")[3],"/")[1],":")[1]
				alloc, err := strconv.ParseFloat(allocStr, 64)
				if err != nil {
					log.Error("Failed to parse alloc: ", err)
					continue
				}

				if _, exists := gpu_usages[splicer]; !exists {
					gpu_usages[splicer] = &GPUsUsageMetrics{}
				}

				gpu_usages[splicer].alloc += alloc
		}
	}
	return gpu_usages
}


/*
 * Implement the Prometheus Collector interface and feed the
 * Slurm scheduler metrics into it.
 * https://godoc.org/github.com/prometheus/client_golang/prometheus#Collector
 */
type GPUsUsageCollector struct {
	alloc       *prometheus.Desc
}

func NewGPUsUsageCollector() *GPUsUsageCollector {
	labels := []string{"user", "node"}
	return &GPUsUsageCollector {
		alloc: prometheus.NewDesc("slurm_gpus_alloc_user", "Allocated GPUs", labels, nil),
	}
}

// Send all metric descriptions
func (cc *GPUsUsageCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- cc.alloc
}

func (cc *GPUsUsageCollector) Collect(ch chan<- prometheus.Metric) {
	cm := ParseAllocatedGPUsUsage(GPUsUsageData())
	for c := range cm {
		user := strings.Split(c,":")[0]
		node := strings.Split(c,":")[1]
		ch <- prometheus.MustNewConstMetric(cc.alloc, prometheus.GaugeValue, cm[c].alloc, user, node)
	}
}
