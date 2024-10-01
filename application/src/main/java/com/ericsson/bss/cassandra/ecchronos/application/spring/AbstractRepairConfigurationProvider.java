/*
 * Copyright 2024 Telefonaktiebolaget LM Ericsson
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ericsson.bss.cassandra.ecchronos.application.spring;

import com.ericsson.bss.cassandra.ecchronos.application.config.Config;
import com.ericsson.bss.cassandra.ecchronos.core.repair.config.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import java.util.HashSet;
import java.util.Set;

import org.springframework.context.ApplicationContext;

public abstract class AbstractRepairConfigurationProvider
{
    private final ApplicationContext applicationContext;

    public final ApplicationContext getApplicationContext()
    {
        return applicationContext;
    }

    private final RepairConfiguration defaultRepairConfiguration;

    protected AbstractRepairConfigurationProvider(final ApplicationContext anApplicationContext)
    {
        this.applicationContext = anApplicationContext;

        Config config = applicationContext.getBean(Config.class);
        this.defaultRepairConfiguration = config.getRepairConfig().asRepairConfiguration();
    }

    public final Set<RepairConfiguration> get(final TableReference tableReference)
    {
        Set<RepairConfiguration> repairConfigurations = new HashSet<>();
        repairConfigurations.addAll(forTable(tableReference));
        if (repairConfigurations.isEmpty())
        {
            repairConfigurations.add(defaultRepairConfiguration);
        }
        return repairConfigurations;
    }

    public abstract Set<RepairConfiguration> forTable(TableReference tableReference);
}


