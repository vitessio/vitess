/*
Copyright 2020 The Vitess Authors.

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

package vreplication

import "fmt"

var jsonDoc1 = `
    {
        "_id": "5f882c85c74593afb7895a16",
        "index": 0,
        "guid": "452caaef-49a9-4483-b875-2440b90d079b",
        "isActive": true,
        "balance": "$2,636.61",
        "picture": "http://placehold.it/32x32",
        "age": 36,
        "eyeColor": "green",
        "name": "Stephens Paul",
        "gender": "male",
        "company": "QUONATA",
        "email": "stephenspaul@quonata.com",
        "phone": "+1 (826) 542-2203",
        "address": "986 Erasmus Street, Tibbie, West Virginia, 3037",
        "registered": "2020-08-06T08:10:25 -02:00",
		"longUnsignedInt": 18446744073709551615
    }
`

type largeDocCollectionType string

const (
	largeJSONArrayCollection  largeDocCollectionType = "array"
	largeJSONObjectCollection largeDocCollectionType = "object"
)

func repeatJSON(jsonDoc string, times int, typ largeDocCollectionType) string {
	var jsonDocs string
	switch typ {
	case largeJSONArrayCollection:
		jsonDocs = "["
		for times > 0 {
			times--
			jsonDocs += jsonDoc1
			if times != 0 {
				jsonDocs += ","
			}
		}
		jsonDocs += "]"
	case largeJSONObjectCollection:
		jsonDocs = "{"
		for times > 0 {
			times--
			jsonDocs += fmt.Sprintf("\"%d\": %s", times, jsonDoc1)
			if times != 0 {
				jsonDocs += ","
			}
		}
		jsonDocs += "}"

	}
	return jsonDocs
}

var jsonDoc2 = `
[
    {
        "_id": "5f882c85c74593afb7895a16",
        "index": 0,
        "guid": "452caaef-49a9-4483-b875-2440b90d079b",
        "isActive": true,
        "balance": "$2,636.61",
        "picture": "http://placehold.it/32x32",
        "age": 36,
        "eyeColor": "green",
        "name": "Stephens Paul",
        "gender": "male",
        "company": "QUONATA",
        "email": "stephenspaul@quonata.com",
        "phone": "+1 (826) 542-2203",
        "address": "986 Erasmus Street, Tibbie, West Virginia, 3037",
        "about": "Reprehenderit nisi in consequat cupidatat aliqua duis. Esse consequat sit exercitation velit in nulla. Anim culpa commodo labore id veniam elit cillum dolore sunt aliquip. Anim ex ea enim non sunt tempor. Enim duis mollit culpa officia reprehenderit aliqua anim proident laboris consectetur eiusmod.\r\n",
        "registered": "2020-08-06T08:10:25 -02:00",
        "latitude": -31.013461,
        "longitude": 136.055816,
        "tags": [
            "nisi",
            "tempor",
            "dolor",
            "in",
            "ut",
            "culpa",
            "adipisicing"
        ],
        "friends": [
            {
                "id": 0,
                "name": "Bessie Mclean"
            },
            {
                "id": 1,
                "name": "Sharon Salazar"
            },
            {
                "id": 2,
                "name": "Ortega Vazquez"
            }
        ],
        "greeting": "Hello, Stephens Paul! You have 9 unread messages.",
        "favoriteFruit": "strawberry"
    },
    {
        "_id": "5f882c85a0da2ca490afc52b",
        "index": 1,
        "guid": "23da3a18-54fb-484b-b921-1122c3693811",
        "isActive": false,
        "balance": "$1,500.22",
        "picture": "http://placehold.it/32x32",
        "age": 25,
        "eyeColor": "brown",
        "name": "Bradley Hinton",
        "gender": "male",
        "company": "AFFLUEX",
        "email": "bradleyhinton@affluex.com",
        "phone": "+1 (909) 576-3260",
        "address": "527 Dictum Court, Crumpler, District Of Columbia, 6169",
        "about": "Aliqua nulla sunt eiusmod cupidatat do nisi anim elit non ut mollit ex. Eu enim duis proident mollit anim exercitation ut aute. Et reprehenderit laboris dolor laboris ex ullamco consectetur consectetur qui veniam laborum. Magna nisi aute consequat cillum duis id dolor voluptate nulla nulla. Aliquip veniam velit commodo sunt duis ex. Sit deserunt est minim labore aliqua veniam anim elit do adipisicing sit in pariatur. Officia ut anim officia exercitation cupidatat cupidatat dolore incididunt incididunt.\r\n",
        "registered": "2019-07-21T11:02:42 -02:00",
        "latitude": 72.516111,
        "longitude": 96.063721,
        "tags": [
            "ullamco",
            "ut",
            "magna",
            "velit",
            "et",
            "labore",
            "nostrud"
        ],
        "friends": [
            {
                "id": 0,
                "name": "Golden Figueroa"
            },
            {
                "id": 1,
                "name": "Brandy Farmer"
            },
            {
                "id": 2,
                "name": "Rosalind Blevins"
            }
        ],
        "greeting": "Hello, Bradley Hinton! You have 10 unread messages.",
        "favoriteFruit": "strawberry"
    },
    {
        "_id": "5f882c8520a784500e49119e",
        "index": 2,
        "guid": "fdbab820-8330-4f26-9ff8-12bd46c91d86",
        "isActive": false,
        "balance": "$3,385.42",
        "picture": "http://placehold.it/32x32",
        "age": 32,
        "eyeColor": "blue",
        "name": "Kirsten Erickson",
        "gender": "female",
        "company": "KINDALOO",
        "email": "kirstenerickson@kindaloo.com",
        "phone": "+1 (872) 521-3868",
        "address": "196 Madison Street, Woodruff, Virgin Islands, 5496",
        "about": "Et sunt sunt deserunt ad do irure do amet elit cillum id commodo. Quis voluptate excepteur id ea. Sunt enim id irure reprehenderit mollit nostrud ea qui non culpa aute.\r\n",
        "registered": "2015-02-22T06:41:54 -01:00",
        "latitude": 80.290313,
        "longitude": 53.088018,
        "tags": [
            "cupidatat",
            "aliquip",
            "anim",
            "aliqua",
            "elit",
            "commodo",
            "aliquip"
        ],
        "friends": [
            {
                "id": 0,
                "name": "Castaneda Schwartz"
            },
            {
                "id": 1,
                "name": "Gracie Rodriquez"
            },
            {
                "id": 2,
                "name": "Isabel Miles"
            }
        ],
        "greeting": "Hello, Kirsten Erickson! You have 2 unread messages.",
        "favoriteFruit": "apple"
    },
    {
        "_id": "5f882c852eac82920fe459da",
        "index": 3,
        "guid": "e45c2807-2d5f-45e7-a2f9-3b61b9953c64",
        "isActive": true,
        "balance": "$1,785.04",
        "picture": "http://placehold.it/32x32",
        "age": 22,
        "eyeColor": "brown",
        "name": "Bertha Guthrie",
        "gender": "female",
        "company": "ISOSTREAM",
        "email": "berthaguthrie@isostream.com",
        "phone": "+1 (988) 409-3274",
        "address": "392 Elmwood Avenue, Venice, Texas, 2787",
        "about": "Cupidatat do et irure sunt dolore ullamco ullamco fugiat excepteur qui. Ut nostrud in laborum nisi. Exercitation deserunt enim exercitation eiusmod eu ea ullamco commodo do pariatur. Incididunt veniam ad anim et reprehenderit tempor irure commodo reprehenderit esse cupidatat.\r\n",
        "registered": "2019-04-14T01:30:41 -02:00",
        "latitude": 62.215237,
        "longitude": -139.310675,
        "tags": [
            "non",
            "eiusmod",
            "culpa",
            "voluptate",
            "sint",
            "labore",
            "labore"
        ],
        "friends": [
            {
                "id": 0,
                "name": "Oneal Ray"
            },
            {
                "id": 1,
                "name": "Mckenzie Wiley"
            },
            {
                "id": 2,
                "name": "Patsy Hood"
            }
        ],
        "greeting": "Hello, Bertha Guthrie! You have 4 unread messages.",
        "favoriteFruit": "apple"
    },
    {
        "_id": "5f882c8584ae9d7e9946216b",
        "index": 4,
        "guid": "e339c4d9-29ac-43ea-ad83-88a357ee0292",
        "isActive": false,
        "balance": "$3,598.44",
        "picture": "http://placehold.it/32x32",
        "age": 31,
        "eyeColor": "brown",
        "name": "Brooks Gallagher",
        "gender": "male",
        "company": "BESTO",
        "email": "brooksgallagher@besto.com",
        "phone": "+1 (887) 444-3408",
        "address": "652 Winthrop Street, Foscoe, Utah, 1225",
        "about": "Dolor laborum laborum pariatur velit ut aliqua. Non voluptate ipsum do consectetur labore dolore incididunt veniam. Elit cillum ullamco ea officia aliqua excepteur deserunt. Ex aute voluptate consectetur ut magna Lorem dolor aliquip elit sit excepteur quis. Esse consectetur veniam consectetur sit voluptate excepteur.\r\n",
        "registered": "2020-04-03T09:26:11 -02:00",
        "latitude": -42.312109,
        "longitude": 78.139406,
        "tags": [
            "esse",
            "do",
            "dolor",
            "ut",
            "culpa",
            "quis",
            "sint"
        ],
        "friends": [
            {
                "id": 0,
                "name": "Blankenship Rosario"
            },
            {
                "id": 1,
                "name": "Deborah Herman"
            },
            {
                "id": 2,
                "name": "Frieda Hill"
            }
        ],
        "greeting": "Hello, Brooks Gallagher! You have 3 unread messages.",
        "favoriteFruit": "apple"
    },
    {
        "_id": "5f882c85686d444c49efbe23",
        "index": 5,
        "guid": "a53afb9c-0c3c-406b-90d2-130d455e2679",
        "isActive": true,
        "balance": "$1,084.59",
        "picture": "http://placehold.it/32x32",
        "age": 26,
        "eyeColor": "green",
        "name": "Cash Steele",
        "gender": "male",
        "company": "ZOMBOID",
        "email": "cashsteele@zomboid.com",
        "phone": "+1 (851) 482-3446",
        "address": "272 Evergreen Avenue, Hoagland, Arizona, 1110",
        "about": "Incididunt cillum consectetur incididunt labore ex laborum culpa sunt et qui voluptate. Et ea reprehenderit ex amet minim nulla et aliqua dolor veniam fugiat officia laborum non. Aliquip magna id sunt cillum voluptate. Ullamco do ad aliqua dolore esse aliquip velit nisi. Ex tempor voluptate dolor adipisicing dolor laborum duis non ea esse sunt. Ut dolore aliquip voluptate ad nisi minim ullamco est. Aliqua est laboris consequat officia sint proident mollit mollit nisi ut non tempor nisi mollit.\r\n",
        "registered": "2019-07-31T07:48:15 -02:00",
        "latitude": -66.786323,
        "longitude": -172.051939,
        "tags": [
            "ea",
            "eiusmod",
            "aute",
            "id",
            "proident",
            "ea",
            "ut"
        ],
        "friends": [
            {
                "id": 0,
                "name": "Ford Williamson"
            },
            {
                "id": 1,
                "name": "Compton Boyd"
            },
            {
                "id": 2,
                "name": "Snyder Warner"
            }
        ],
        "greeting": "Hello, Cash Steele! You have 2 unread messages.",
        "favoriteFruit": "strawberry"
    }
]
`
