#!/bin/bash

# Copyright 2025 The Vitess Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

echo "=================================================="
echo "🎯 GEO-SHARDING LIVE DEMO"
echo "=================================================="
echo ""
echo "This demo shows how multicol vindex enables:"
echo "✨ Predictable sharding across global regions"
echo "✨ Country-level subsharding within regions"
echo "✨ Efficient query targeting"
echo ""

# Function to execute and show commands
demo_cmd() {
    echo "🔍 EXECUTING: $1"
    echo "---"
    eval "$1"
    echo ""
    echo "Press Enter to continue..."
    read
}

echo "=================================================="
echo "STEP 1: Understanding Our Schema"
echo "=================================================="

demo_cmd "mysql -h 127.0.0.1 -P 15306 -e 'USE main; DESCRIBE customer;'"

echo "=================================================="
echo "STEP 2: The Multicol Vindex Configuration"
echo "=================================================="
echo "Let's see how our vindex is configured:"

demo_cmd "cat main_vschema_multicol.json"

echo "=================================================="
echo "STEP 3: Insert Global Customers"
echo "=================================================="
echo "Let's insert customers with strategic country_id values:"

demo_cmd "cat insert_customers.sql"

echo "Now inserting the data..."
demo_cmd "mysql -h 127.0.0.1 -P 15306 < insert_customers.sql"

echo "=================================================="
echo "STEP 4: The Magic - Keyspace Distribution"
echo "=================================================="
echo "Watch how country_id values map to shard ranges:"

demo_cmd "mysql -h 127.0.0.1 -P 15306 -e \"USE main; SELECT 
    CONCAT('0x', LPAD(HEX(country_id), 2, '0')) as country_hex,
    country_id,
    CASE 
        WHEN country_id BETWEEN 0 AND 63 THEN 'Americas (-40 range)'
        WHEN country_id BETWEEN 64 AND 127 THEN 'Europe (40-80 range)'
        WHEN country_id BETWEEN 128 AND 191 THEN 'APAC (80-C0 range)'
        WHEN country_id BETWEEN 192 AND 255 THEN 'MEA (C0- range)'
        ELSE 'Unknown'
    END as shard_mapping,
    COUNT(*) as customer_count
FROM customer 
GROUP BY country_id 
ORDER BY country_id;\""

echo "=================================================="
echo "STEP 5: Single Shard Targeting"
echo "=================================================="
echo "First, let's see how specific customer queries target single shards:"

echo "Query 5A: Find specific customer by country_id + id (targets single shard)"
demo_cmd "mysql -h 127.0.0.1 -P 15306 -e \"USE main; SELECT country_id, id, email, 'Single shard target' as targeting FROM customer WHERE country_id = 75 AND id = 202;\""

echo "Query 5B: Find another specific customer (different shard)"
demo_cmd "mysql -h 127.0.0.1 -P 15306 -e \"USE main; SELECT country_id, id, email, 'Single shard target' as targeting FROM customer WHERE country_id = 135 AND id = 301;\""

echo "Query 5C: Find customer in MEA region (yet another shard)"
demo_cmd "mysql -h 127.0.0.1 -P 15306 -e \"USE main; SELECT country_id, id, email, 'Single shard target' as targeting FROM customer WHERE country_id = 200 AND id = 401;\""

echo ""
echo "Now let's see how country-only queries target entire regions efficiently:"

echo "Query 5D: All customers from Germany (targets Europe shard only)"
demo_cmd "mysql -h 127.0.0.1 -P 15306 -e \"USE main; SELECT country_id, id, email, 'Germany region query' as targeting FROM customer WHERE country_id = 75 ORDER BY id;\""

echo "Query 5E: All customers from India (targets APAC shard only)" 
demo_cmd "mysql -h 127.0.0.1 -P 15306 -e \"USE main; SELECT country_id, id, email, 'India region query' as targeting FROM customer WHERE country_id = 135 ORDER BY id;\""


echo "=================================================="
echo "🌍 GEO-SHARDING DEMO COMPLETE!"
echo "=================================================="
echo ""
echo "What we achieved with multicol vindex geo-sharding:"
echo ""
echo "🗺️  GEOGRAPHIC DATA ISOLATION"
echo "   • Americas data stays in Americas shard"
echo "   • European data stays in European shard"
echo "   • APAC data stays in APAC shard"
echo "   • MEA data stays in MEA shard"
echo ""
echo "🎯 EFFICIENT GEO-TARGETING"
echo "   • Single customer lookup → hits one geographic shard"
echo "   • Country-wide queries → stay within regional boundaries"
echo "   • No cross-region data access for local operations"
echo ""
echo "⚖️  COMPLIANCE & PERFORMANCE"
echo "   • Data residency requirements automatically satisfied"
echo "   • Predictable latency within geographic regions"
echo "   • Scalable: add countries without resharding"
echo ""
echo "🚀 Ready for global-scale applications with geographic constraints!"
echo ""