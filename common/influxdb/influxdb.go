// Copyright 2015 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package influxdb

import (
	"bufio"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	influxdb "github.com/influxdata/influxdb/client"
	"k8s.io/heapster/version"
)

type InfluxdbClient interface {
	Write(influxdb.BatchPoints) (*influxdb.Response, error)
	Query(influxdb.Query) (*influxdb.Response, error)
	Ping() (time.Duration, string, error)
}

type InfluxdbConfig struct {
	User       string
	Password   string
	Secure     bool
	Host       string
	DbName     string
	WithFields bool
}

func NewClient(c InfluxdbConfig) (InfluxdbClient, error) {
	url := &url.URL{
		Scheme: "http",
		Host:   c.Host,
	}
	if c.Secure {
		url.Scheme = "https"
	}

	iConfig := &influxdb.Config{
		URL:       *url,
		Username:  c.User,
		Password:  c.Password,
		UserAgent: fmt.Sprintf("%v/%v", "heapster", version.HeapsterVersion),
	}
	client, err := influxdb.NewClient(*iConfig)

	if err != nil {
		return nil, err
	}
	if _, _, err := client.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping InfluxDB server at %q - %v", c.Host, err)
	}
	return client, nil
}

func BuildConfig(uri *url.URL) (*InfluxdbConfig, error) {
	// Read influxdb config from secret @MS
	config, err := readConfig("/srv/influxdb/secrets/.admin")
	if err != nil {
		config = InfluxdbConfig{
			User:       "root",
			Password:   "root",
			Host:       "localhost:8086",
			DbName:     "k8s",
			Secure:     false,
			WithFields: false,
		}
	}

	if len(uri.Host) > 0 {
		config.Host = uri.Host
	}
	opts := uri.Query()
	if len(opts["user"]) >= 1 {
		config.User = opts["user"][0]
	}
	// TODO: use more secure way to pass the password.
	if len(opts["pw"]) >= 1 {
		config.Password = opts["pw"][0]
	}
	if len(opts["db"]) >= 1 {
		config.DbName = opts["db"][0]
	}
	if len(opts["withfields"]) >= 1 {
		val, err := strconv.ParseBool(opts["withfields"][0])
		if err != nil {
			return nil, fmt.Errorf("failed to parse `withfields` flag - %v", err)
		}
		config.WithFields = val
	}
	if len(opts["secure"]) >= 1 {
		val, err := strconv.ParseBool(opts["secure"][0])
		if err != nil {
			return nil, fmt.Errorf("failed to parse `secure` flag - %v", err)
		}
		config.Secure = val
	}

	return &config, nil
}

// Read Influxdb Config from file @MS
func readConfig(path string) (InfluxdbConfig, error) {
	file, err := os.Open(path)
	if err != nil {
		return InfluxdbConfig{}, err
	}
	defer file.Close()
	mp := make(map[string]string)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		result := strings.Split(scanner.Text(), "=")
		if len(result) != 2 {
			continue
		}
		mp[result[0]] = result[1]
	}
	// Default
	var user, password string
	host := "monitoring-influxdb.default:8086"
	dbName := "k8s"

	if u, ok := mp["INFLUX_WRITE_USER"]; ok {
		user = u
	} else {
		return InfluxdbConfig{}, errors.New("INFLUX_WRITE_USER not found")
	}
	if p, ok := mp["INFLUX_WRITE_PASSWORD"]; ok {
		password = p
	} else {
		return InfluxdbConfig{}, errors.New("INFLUX_WRITE_PASSWORD not found")
	}
	if h, ok := mp["INFLUX_HOST"]; ok {
		if p, ok := mp["INFLUX_API_PORT"]; ok {
			host = h + ":" + p
		}
	}
	if d, ok := mp["INFLUX_DB"]; ok {
		dbName = d
	}

	defaultConfig := InfluxdbConfig{
		User:     user,
		Password: password,
		Host:     host,
		DbName:   dbName,
		Secure:   false,
	}
	return defaultConfig, nil
}
