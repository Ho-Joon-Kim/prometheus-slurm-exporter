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

type GPUsMetrics struct {
	alloc       float64
	idle        float64
	total       float64
	utilization float64
}



func ParseAllocatedGPUs(input []byte) float64 {
	var num_gpus = 0.0

	lines := strings.Split(string(input), "\n")
	for _, line := range lines {
		if strings.Contains(line,"|") {
				allocStr := strings.Split(strings.Split(strings.Split(line,"|")[3],"/")[1],":")[1]
				alloc, err := strconv.ParseFloat(allocStr, 64)
				if err != nil {
					log.Error("Failed to parse alloc: ", err)
					continue
				}
				num_gpus += alloc
		}
	}

	return num_gpus
}

func ParseTotalGPUs() map[string]*float64 {
	num_gpus := make(map[string]*float64)

	args := []string{"-h", "-o \"%n %G\""}
	output := string(Execute("sinfo", args))
	if len(output) > 0 {
		for _, line := range strings.Split(output, "\n") {
			if len(line) > 0 {
				line = strings.Trim(line, "\"")
				node_name := strings.Split(line, " ")[0]
				descriptor := strings.Fields(line)[1]
				descriptor = strings.TrimPrefix(descriptor, "gpu:")
				descriptor = strings.Split(descriptor, ":")[1]
				gpu_name := strings.Split(descriptor, ":")[0]
				node_gpus, _ :=  strconv.ParseFloat(descriptor, 64)

				num_gpus[node_name + ":" + gpu_name] = &node_gpus
			}
		}
	}

	return num_gpus
}

func ParseGPUsMetrics() map[string]*GPUsMetrics {
	gpu_usages := make(map[string]*GPUsMetrics)

	total_gpus_nodes := ParseTotalGPUs()

	for node, total_gpus := range total_gpus_nodes {
		node_name := strings.Split(node, ":")[0]
		// gpu_name := strings.Split(node, ":")[1]

		allocated_gpus := ParseAllocatedGPUs(GPUsUsageData(node_name))

		if _, exists := gpu_usages[node]; !exists {
			gpu_usages[node] = &GPUsMetrics{}
		}

		gpu_usages[node].alloc = allocated_gpus
		gpu_usages[node].idle = *total_gpus - allocated_gpus
		gpu_usages[node].total = *total_gpus
		gpu_usages[node].utilization = allocated_gpus / *total_gpus
	}
	return gpu_usages
}

// Execute the sinfo command and return its output
func Execute(command string, arguments []string) []byte {
	cmd := exec.Command(command, arguments...)
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

/*
 * Implement the Prometheus Collector interface and feed the
 * Slurm scheduler metrics into it.
 * https://godoc.org/github.com/prometheus/client_golang/prometheus#Collector
 */

func NewGPUsCollector() *GPUsCollector {
	labels := []string{"node", "model"}
	return &GPUsCollector{
		alloc: prometheus.NewDesc("slurm_gpus_alloc", "Allocated GPUs", labels, nil),
		idle:  prometheus.NewDesc("slurm_gpus_idle", "Idle GPUs", labels, nil),
		total: prometheus.NewDesc("slurm_gpus_total", "Total GPUs", labels, nil),
		utilization: prometheus.NewDesc("slurm_gpus_utilization", "Total GPU utilization", labels, nil),
	}
}

type GPUsCollector struct {
	alloc       *prometheus.Desc
	idle        *prometheus.Desc
	total       *prometheus.Desc
	utilization *prometheus.Desc
}

// Send all metric descriptions
func (cc *GPUsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- cc.alloc
	ch <- cc.idle
	ch <- cc.total
	ch <- cc.utilization
}
func (cc *GPUsCollector) Collect(ch chan<- prometheus.Metric) {
	cm := ParseGPUsMetrics()
	for c := range cm {
		node := strings.Split(c,":")[0]
		gpu_model := strings.Split(c,":")[1]
		ch <- prometheus.MustNewConstMetric(cc.alloc, prometheus.GaugeValue, cm[c].alloc, node, gpu_model)
		ch <- prometheus.MustNewConstMetric(cc.idle, prometheus.GaugeValue, cm[c].idle, node, gpu_model)
		ch <- prometheus.MustNewConstMetric(cc.total, prometheus.GaugeValue, cm[c].total, node, gpu_model)
		ch <- prometheus.MustNewConstMetric(cc.utilization, prometheus.GaugeValue, cm[c].utilization, node, gpu_model)
	}
}
