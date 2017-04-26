/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.repository.typestore;

import com.google.inject.Inject;
import org.apache.atlas.typesystem.types.cache.TypeCache;
import org.testng.Assert;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

/**
 *  Verify Guice can successfully instantiate and inject StoreBackTypeCache.
 *  StoreBackedTypeCacheTestModule Guice module uses Atlas configuration
 *  which has type cache implementation class set to {@link StoreBackedTypeCache}.
 */
@Guice(modules = StoreBackedTypeCacheTestOnlyModule.class)
public class StoreBackedTypeCacheConfigurationTest {

    @Inject
    private TypeCache typeCache;

    @Test
    public void testConfigureAsTypeCache() throws Exception {
        // Verify Guice successfully instantiated and injected StoreBackTypeCache
        Assert.assertTrue(typeCache instanceof StoreBackedTypeCache);
    }
}