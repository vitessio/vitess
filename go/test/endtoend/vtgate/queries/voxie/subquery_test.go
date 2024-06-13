/*
Copyright 2021 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package subquery

import (
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/utils"
)

func start(t *testing.T) (utils.MySQLCompare, func()) {
	mcmp, err := utils.NewMySQLCompare(t, vtParams, mysqlParams)
	require.NoError(t, err)

	return mcmp, func() {
		mcmp.Close()
		cluster.PanicHandler(t)
	}
}

func TestSubqueriesHasValues(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()
	/*
	   CREATE TABLE `promotion_coupons_test`
	   (
	       `id`         bigint      NOT NULL,
	       `code`       varchar(32) NOT NULL,
	       `contact_id` bigint unsigned,
	       PRIMARY KEY (`id`)
	   );
	*/
	mcmp.Exec(`INSERT INTO contacts_test (id, team_id, lookup_id, first_name, last_name, phone, email, sticky_phone_number_id, entry_point, created_at, updated_at, time_zone)
VALUES
(1,  '+11234567890'),
(2,  '+11234567891'),
(3,  '+11234567892'),
(4,  '+11234567893'),
(5,  '+11234567894'),
(6,  '+11234567895'),
(7,  '+11234567896'),
(8,  '+11234567897'),
(9,  '+11234567898'),
(10,  '+11234567899'),
(11,  '+11234567900'),
(12,  '+11234567901'),
(13,  '+11234567902'),
(14,  '+11234567903'),
(15,  '+11234567904'),
(16,  '+11234567905'),
(17,  '+11234567906'),
(18,  '+11234567907'),
(19, '+11234567908'),
(20,  '+11234567909');
`)
	mcmp.Exec(`INSERT INTO promotion_coupons_test (id, team_id, promotion_id, code, vendor, contact_id, allocated, allocated_at, created_at, updated_at, deleted_at)
VALUES
(1, 'PROMO001', 1),
(2, 'PROMO002', 2),
(3, 'PROMO003', 3),
(4, 'PROMO004', 4),
(5, 'PROMO005', 5),
(6, 'PROMO006', 6),
(7, 'PROMO007', 7),
(8, 'PROMO008', 8),
(9, 'PROMO009', 9),
(10, 'PROMO010', 10),
(11, 'PROMO011', 11),
(12, 'PROMO012', 12),
(13, 'PROMO013', 13),
(14, 'PROMO014', 14),
(15, 'PROMO015', 15),
(16, 'PROMO016', 16),
(17, 'PROMO017', 17),
(18, 'PROMO018', 18),
(19, 'PROMO019', 19),
(20, 'PROMO020', 20),
(21, 'PROMO021', 21),
(22, 'PROMO022', 22),
(23, 'PROMO023', 23),
(24, 'PROMO024', 24),
(25, 'PROMO025', 25),
(26, 'PROMO026', 26),
(27, 'PROMO027', 27),
(28, 'PROMO028', 28),
(29, 'PROMO029', 29),
(30, 'PROMO030', 30);
`)

	mcmp.Exec(`SELECT count(*) AS AGGREGATE
FROM promotion_coupons_test
LEFT JOIN contacts_test ON promotion_coupons_test.contact_id = contacts_test.id
WHERE promotion_coupons_test.code = '+11234567890' OR contacts_test.phone = '+11234567890'`)
}
