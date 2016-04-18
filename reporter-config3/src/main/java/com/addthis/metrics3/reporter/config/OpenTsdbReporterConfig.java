/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.addthis.metrics3.reporter.config;

import com.addthis.metrics.reporter.config.AbstractOpenTsdbReporterConfig;
import com.addthis.metrics.reporter.config.HostPort;
import com.codahale.metrics.MetricRegistry;
import com.github.sps.metrics.OpenTsdbReporter;
import com.github.sps.metrics.opentsdb.OpenTsdb;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class OpenTsdbReporterConfig extends AbstractOpenTsdbReporterConfig implements MetricsReporterConfigThree {
  private static final String REPORTER_CLASS = "com.github.sps.metrics.OpenTsdbReporter";
  private static final Logger log = LoggerFactory.getLogger(OpenTsdbReporterConfig.class);

  private List<OpenTsdbReporter> reporters = new ArrayList<OpenTsdbReporter>();

  @Override
  public boolean enable(MetricRegistry registry) {
    if (!setup(REPORTER_CLASS)) {
      return false;
    }
    boolean failures = false;
    for (HostPort hostPort : getFullHostList()) {
      try {
        log.info("Enabling OpenTsdbReporter to {}:{}",
                new Object[]{hostPort.getHost(), hostPort.getPort()});
        String baseUrl = "http://" + hostPort.getHost() + ":" + hostPort.getPort();
        OpenTsdbReporter reporter = OpenTsdbReporter.forRegistry(registry)
                .convertRatesTo(getRealRateunit())
                .convertDurationsTo(getRealDurationunit())
                .prefixedWith(getResolvedPrefix())
                .filter(MetricFilterTransformer.generateFilter(getPredicate()))
                .build(new OpenTsdb.Builder(baseUrl).create());
        reporter.start(getPeriod(), getRealTimeunit());
        reporters.add(reporter);
      } catch (Exception e) {
        log.error("Failed to enable OpenTsdbReporter to {}:{}",
                new Object[]{hostPort.getHost(), hostPort.getPort()}, e);
        failures = true;
      }
    }
    return !failures;
  }

  @Override
  public void report() {
    for (OpenTsdbReporter reporter : reporters) {
      reporter.report();
    }
  }

  void stopForTests() {
    for (OpenTsdbReporter reporter : reporters) {
      reporter.stop();
    }
  }

}
