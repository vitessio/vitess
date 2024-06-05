package schemadiff

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/capabilities"
	"vitess.io/vitess/go/vt/sqlparser"
)

func TestAlterTableCapableOfInstantDDL(t *testing.T) {
	capableOf := func(capability capabilities.FlavorCapability) (bool, error) {
		switch capability {
		case
			capabilities.InstantDDLFlavorCapability,
			capabilities.InstantAddLastColumnFlavorCapability,
			capabilities.InstantAddDropVirtualColumnFlavorCapability,
			capabilities.InstantAddDropColumnFlavorCapability,
			capabilities.InstantChangeColumnDefaultFlavorCapability,
			capabilities.InstantExpandEnumCapability:
			return true, nil
		}
		return false, nil
	}
	incapableOf := func(capability capabilities.FlavorCapability) (bool, error) {
		return false, nil
	}
	parser := sqlparser.NewTestParser()

	tcases := []struct {
		name                      string
		create                    string
		alter                     string
		expectCapableOfInstantDDL bool
		capableOf                 capabilities.CapableOf
	}{
		{
			name:                      "add column",
			create:                    "create table t1 (id int, i1 int)",
			alter:                     "alter table t1 add column i2 int",
			expectCapableOfInstantDDL: true,
		},
		{
			name:                      "add multiple columns",
			create:                    "create table t1 (id int, i1 int)",
			alter:                     "alter table t1 add column i2 int, add column i3 int, add column i4 int",
			expectCapableOfInstantDDL: true,
		},
		{
			name:                      "add last column",
			create:                    "create table t1 (id int, i1 int)",
			alter:                     "alter table t1 add column i2 int after i1",
			expectCapableOfInstantDDL: true,
		},
		{
			name:                      "add mid column",
			create:                    "create table t1 (id int, i1 int)",
			alter:                     "alter table t1 add column i2 int after id",
			expectCapableOfInstantDDL: true,
		},
		{
			name:                      "add mid column, incapable",
			create:                    "create table t1 (id int, i1 int)",
			alter:                     "alter table t1 add column i2 int after id",
			capableOf:                 incapableOf,
			expectCapableOfInstantDDL: false,
		},
		{
			name:                      "add column fails on COMPRESSED tables",
			create:                    "create table t1 (id int, i1 int) row_format=compressed",
			alter:                     "alter table t1 add column i2 int",
			expectCapableOfInstantDDL: false,
		},
		{
			name:                      "add column fails on table with FULLTEXT index",
			create:                    "create table t(id int, name varchar(128), primary key(id), fulltext key (name))",
			alter:                     "alter table t1 add column i2 int",
			expectCapableOfInstantDDL: false,
		},
		{
			name:                      "add column with default value",
			create:                    "create table t1 (id int, i1 int)",
			alter:                     "alter table t1 add column i2 int not null default 17",
			expectCapableOfInstantDDL: true,
		},
		{
			name:                      "add column with expression default value",
			create:                    "create table t1 (id int, i1 int)",
			alter:                     "alter table t1 add column i2 int not null default (17)",
			expectCapableOfInstantDDL: false,
		},
		{
			name:                      "add column with complex expression default value",
			create:                    "create table t1 (id int, i1 int)",
			alter:                     "alter table t1 add column i2 int not null default ((17+2))",
			expectCapableOfInstantDDL: false,
		},
		{
			name:                      "add varchar column with default literal value",
			create:                    "create table t1 (id int, i1 int)",
			alter:                     "alter table t1 add column v2 varchar(10) not null default '17'",
			expectCapableOfInstantDDL: true,
		},
		{
			name:                      "add varchar column with default expression value",
			create:                    "create table t1 (id int, i1 int)",
			alter:                     "alter table t1 add column v2 varchar(10) not null default ('17')",
			expectCapableOfInstantDDL: false,
		},
		{
			name:                      "add varchar column with default expression null value",
			create:                    "create table t1 (id int, i1 int)",
			alter:                     "alter table t1 add column v2 varchar(10) not null default (null)",
			expectCapableOfInstantDDL: false,
		},
		{
			name: "add columns max capacity",
			create: `create table t(i1 int, i2 int, i3 int, i4 int, i5 int, i6 int, i7 int, i8 int, i9 int, i10 int, i11 int, i12 int, i13 int, i14 int, i15 int, i16 int, i17 int, i18 int, i19 int, i20 int, i21 int, i22 int, i23 int, i24 int, i25 int, i26 int, i27 int, i28 int, i29 int, i30 int, i31 int, i32 int, i33 int, i34 int, i35 int, i36 int, i37 int, i38 int, i39 int, i40 int, i41 int, i42 int, i43 int, i44 int, i45 int, i46 int, i47 int, i48 int, i49 int, i50 int, i51 int, i52 int, i53 int, i54 int, i55 int, i56 int, i57 int, i58 int, i59 int, i60 int, i61 int, i62 int, i63 int, i64 int, i65 int, i66 int, i67 int, i68 int, i69 int, i70 int, i71 int, i72 int, i73 int, i74 int, i75 int, i76 int, i77 int, i78 int, i79 int, i80 int, i81 int, i82 int, i83 int, i84 int, i85 int, i86 int, i87 int, i88 int, i89 int, i90 int, i91 int, i92 int, i93 int, i94 int, i95 int, i96 int, i97 int, i98 int, i99 int, i100 int, i101 int, i102 int, i103 int, i104 int, i105 int, i106 int, i107 int, i108 int, i109 int, i110 int, i111 int, i112 int, i113 int, i114 int, i115 int, i116 int, i117 int, i118 int, i119 int, i120 int, i121 int, i122 int, i123 int, i124 int, i125 int, i126 int, i127 int, i128 int, i129 int, i130 int, i131 int, i132 int, i133 int, i134 int, i135 int, i136 int, i137 int, i138 int, i139 int, i140 int, i141 int, i142 int, i143 int, i144 int, i145 int, i146 int, i147 int, i148 int, i149 int, i150 int, i151 int, i152 int, i153 int, i154 int, i155 int, i156 int, i157 int, i158 int, i159 int, i160 int, i161 int, i162 int, i163 int, i164 int, i165 int, i166 int, i167 int, i168 int, i169 int, i170 int, i171 int, i172 int, i173 int, i174 int, i175 int, i176 int, i177 int, i178 int, i179 int, i180 int, i181 int, i182 int, i183 int, i184 int, i185 int, i186 int, i187 int, i188 int, i189 int, i190 int, i191 int, i192 int, i193 int, i194 int, i195 int, i196 int, i197 int, i198 int, i199 int, i200 int,
				i201 int, i202 int, i203 int, i204 int, i205 int, i206 int, i207 int, i208 int, i209 int, i210 int, i211 int, i212 int, i213 int, i214 int, i215 int, i216 int, i217 int, i218 int, i219 int, i220 int, i221 int, i222 int, i223 int, i224 int, i225 int, i226 int, i227 int, i228 int, i229 int, i230 int, i231 int, i232 int, i233 int, i234 int, i235 int, i236 int, i237 int, i238 int, i239 int, i240 int, i241 int, i242 int, i243 int, i244 int, i245 int, i246 int, i247 int, i248 int, i249 int, i250 int, i251 int, i252 int, i253 int, i254 int, i255 int, i256 int, i257 int, i258 int, i259 int, i260 int, i261 int, i262 int, i263 int, i264 int, i265 int, i266 int, i267 int, i268 int, i269 int, i270 int, i271 int, i272 int, i273 int, i274 int, i275 int, i276 int, i277 int, i278 int, i279 int, i280 int, i281 int, i282 int, i283 int, i284 int, i285 int, i286 int, i287 int, i288 int, i289 int, i290 int, i291 int, i292 int, i293 int, i294 int, i295 int, i296 int, i297 int, i298 int, i299 int, i300 int, i301 int, i302 int, i303 int, i304 int, i305 int, i306 int, i307 int, i308 int, i309 int, i310 int, i311 int, i312 int, i313 int, i314 int, i315 int, i316 int, i317 int, i318 int, i319 int, i320 int, i321 int, i322 int, i323 int, i324 int, i325 int, i326 int, i327 int, i328 int, i329 int, i330 int, i331 int, i332 int, i333 int, i334 int, i335 int, i336 int, i337 int, i338 int, i339 int, i340 int, i341 int, i342 int, i343 int, i344 int, i345 int, i346 int, i347 int, i348 int, i349 int, i350 int, i351 int, i352 int, i353 int, i354 int, i355 int, i356 int, i357 int, i358 int, i359 int, i360 int, i361 int, i362 int, i363 int, i364 int, i365 int, i366 int, i367 int, i368 int, i369 int, i370 int, i371 int, i372 int, i373 int, i374 int, i375 int, i376 int, i377 int, i378 int, i379 int, i380 int, i381 int, i382 int, i383 int, i384 int, i385 int, i386 int, i387 int, i388 int, i389 int, i390 int, i391 int, i392 int, i393 int, i394 int, i395 int, i396 int, i397 int, i398 int, i399 int,
				i400 int, i401 int, i402 int, i403 int, i404 int, i405 int, i406 int, i407 int, i408 int, i409 int, i410 int, i411 int, i412 int, i413 int, i414 int, i415 int, i416 int, i417 int, i418 int, i419 int, i420 int, i421 int, i422 int, i423 int, i424 int, i425 int, i426 int, i427 int, i428 int, i429 int, i430 int, i431 int, i432 int, i433 int, i434 int, i435 int, i436 int, i437 int, i438 int, i439 int, i440 int, i441 int, i442 int, i443 int, i444 int, i445 int, i446 int, i447 int, i448 int, i449 int, i450 int, i451 int, i452 int, i453 int, i454 int, i455 int, i456 int, i457 int, i458 int, i459 int, i460 int, i461 int, i462 int, i463 int, i464 int, i465 int, i466 int, i467 int, i468 int, i469 int, i470 int, i471 int, i472 int, i473 int, i474 int, i475 int, i476 int, i477 int, i478 int, i479 int, i480 int, i481 int, i482 int, i483 int, i484 int, i485 int, i486 int, i487 int, i488 int, i489 int, i490 int, i491 int, i492 int, i493 int, i494 int, i495 int, i496 int, i497 int, i498 int, i499 int, i500 int, i501 int, i502 int, i503 int, i504 int, i505 int, i506 int, i507 int, i508 int, i509 int, i510 int, i511 int, i512 int, i513 int, i514 int, i515 int, i516 int, i517 int, i518 int, i519 int, i520 int, i521 int, i522 int, i523 int, i524 int, i525 int, i526 int, i527 int, i528 int, i529 int, i530 int, i531 int, i532 int, i533 int, i534 int, i535 int, i536 int, i537 int, i538 int, i539 int, i540 int, i541 int, i542 int, i543 int, i544 int, i545 int, i546 int, i547 int, i548 int, i549 int, i550 int, i551 int, i552 int, i553 int, i554 int, i555 int, i556 int, i557 int, i558 int, i559 int, i560 int, i561 int, i562 int, i563 int, i564 int, i565 int, i566 int, i567 int, i568 int, i569 int, i570 int, i571 int, i572 int, i573 int, i574 int, i575 int, i576 int, i577 int, i578 int, i579 int, i580 int, i581 int, i582 int, i583 int, i584 int, i585 int, i586 int, i587 int, i588 int, i589 int, i590 int, i591 int, i592 int, i593 int, i594 int, i595 int, i596 int, i597 int, i598 int, i599 int,
				i600 int, i601 int, i602 int, i603 int, i604 int, i605 int, i606 int, i607 int, i608 int, i609 int, i610 int, i611 int, i612 int, i613 int, i614 int, i615 int, i616 int, i617 int, i618 int, i619 int, i620 int, i621 int, i622 int, i623 int, i624 int, i625 int, i626 int, i627 int, i628 int, i629 int, i630 int, i631 int, i632 int, i633 int, i634 int, i635 int, i636 int, i637 int, i638 int, i639 int, i640 int, i641 int, i642 int, i643 int, i644 int, i645 int, i646 int, i647 int, i648 int, i649 int, i650 int, i651 int, i652 int, i653 int, i654 int, i655 int, i656 int, i657 int, i658 int, i659 int, i660 int, i661 int, i662 int, i663 int, i664 int, i665 int, i666 int, i667 int, i668 int, i669 int, i670 int, i671 int, i672 int, i673 int, i674 int, i675 int, i676 int, i677 int, i678 int, i679 int, i680 int, i681 int, i682 int, i683 int, i684 int, i685 int, i686 int, i687 int, i688 int, i689 int, i690 int, i691 int, i692 int, i693 int, i694 int, i695 int, i696 int, i697 int, i698 int, i699 int, i700 int, i701 int, i702 int, i703 int, i704 int, i705 int, i706 int, i707 int, i708 int, i709 int, i710 int, i711 int, i712 int, i713 int, i714 int, i715 int, i716 int, i717 int, i718 int, i719 int, i720 int, i721 int, i722 int, i723 int, i724 int, i725 int, i726 int, i727 int, i728 int, i729 int, i730 int, i731 int, i732 int, i733 int, i734 int, i735 int, i736 int, i737 int, i738 int, i739 int, i740 int, i741 int, i742 int, i743 int, i744 int, i745 int, i746 int, i747 int, i748 int, i749 int, i750 int, i751 int, i752 int, i753 int, i754 int, i755 int, i756 int, i757 int, i758 int, i759 int, i760 int, i761 int, i762 int, i763 int, i764 int, i765 int, i766 int, i767 int, i768 int, i769 int, i770 int, i771 int, i772 int, i773 int, i774 int, i775 int, i776 int, i777 int, i778 int, i779 int, i780 int, i781 int, i782 int, i783 int, i784 int, i785 int, i786 int, i787 int, i788 int, i789 int, i790 int, i791 int, i792 int, i793 int, i794 int, i795 int, i796 int, i797 int, i798 int, i799 int,
				i800 int, i801 int, i802 int, i803 int, i804 int, i805 int, i806 int, i807 int, i808 int, i809 int, i810 int, i811 int, i812 int, i813 int, i814 int, i815 int, i816 int, i817 int, i818 int, i819 int, i820 int, i821 int, i822 int, i823 int, i824 int, i825 int, i826 int, i827 int, i828 int, i829 int, i830 int, i831 int, i832 int, i833 int, i834 int, i835 int, i836 int, i837 int, i838 int, i839 int, i840 int, i841 int, i842 int, i843 int, i844 int, i845 int, i846 int, i847 int, i848 int, i849 int, i850 int, i851 int, i852 int, i853 int, i854 int, i855 int, i856 int, i857 int, i858 int, i859 int, i860 int, i861 int, i862 int, i863 int, i864 int, i865 int, i866 int, i867 int, i868 int, i869 int, i870 int, i871 int, i872 int, i873 int, i874 int, i875 int, i876 int, i877 int, i878 int, i879 int, i880 int, i881 int, i882 int, i883 int, i884 int, i885 int, i886 int, i887 int, i888 int, i889 int, i890 int, i891 int, i892 int, i893 int, i894 int, i895 int, i896 int, i897 int, i898 int, i899 int, i900 int, i901 int, i902 int, i903 int, i904 int, i905 int, i906 int, i907 int, i908 int, i909 int, i910 int, i911 int, i912 int, i913 int, i914 int, i915 int, i916 int, i917 int, i918 int, i919 int, i920 int, i921 int, i922 int, i923 int, i924 int, i925 int, i926 int, i927 int, i928 int, i929 int, i930 int, i931 int, i932 int, i933 int, i934 int, i935 int, i936 int, i937 int, i938 int, i939 int, i940 int, i941 int, i942 int, i943 int, i944 int, i945 int, i946 int, i947 int, i948 int, i949 int, i950 int, i951 int, i952 int, i953 int, i954 int, i955 int, i956 int, i957 int, i958 int, i959 int, i960 int, i961 int, i962 int, i963 int, i964 int, i965 int, i966 int, i967 int, i968 int, i969 int, i970 int, i971 int, i972 int, i973 int, i974 int, i975 int, i976 int, i977 int, i978 int, i979 int, i980 int, i981 int, i982 int, i983 int, i984 int, i985 int, i986 int, i987 int, i988 int, i989 int, i990 int, i991 int, i992 int, i993 int, i994 int, i995 int, i996 int, i997 int, i998 int, i999 int,
				i1000 int, i1001 int, i1002 int, i1003 int, i1004 int, i1005 int, i1006 int, i1007 int, i1008 int, i1009 int, i1010 int, i1011 int, i1012 int, i1013 int, i1014 int, i1015 int, i1016 int, i1017 int, i1018 int, i1019 int, i1020 int, i1021 int)`,
			alter:                     "alter table t1 add column j1 int, add column j2 int",
			expectCapableOfInstantDDL: false,
		},
		{
			name:                      "add virtual column",
			create:                    "create table t(id int, i1 int not null, primary key(id))",
			alter:                     "alter table t add column i2 int generated always as (i1 + 1) virtual",
			expectCapableOfInstantDDL: true,
		},
		{
			name:                      "add stored column",
			create:                    "create table t(id int, i1 int not null, primary key(id))",
			alter:                     "alter table t add column i2 int generated always as (i1 + 1) stored",
			expectCapableOfInstantDDL: false,
		},
		{
			name:                      "drop virtual column",
			create:                    "create table t(id int, i1 int not null, i2 int generated always as (i1 + 1) virtual, primary key(id))",
			alter:                     "alter table t drop column i2",
			expectCapableOfInstantDDL: true,
		},
		{
			name:                      "drop stored virtual column",
			create:                    "create table t(id int, i1 int not null, i2 int generated always as (i1 + 1) stored, primary key(id))",
			alter:                     "alter table t drop column i2",
			expectCapableOfInstantDDL: true,
		},
		{
			name:                      "drop mid column",
			create:                    "create table t(id int, i1 int not null, i2 int not null, primary key(id))",
			alter:                     "alter table t drop column i1",
			expectCapableOfInstantDDL: true,
		},
		{
			name:                      "fail due to row_format=compressed",
			create:                    "create table t(id int, i1 int not null, i2 int not null, primary key(id)) row_format=compressed",
			alter:                     "alter table t drop column i1",
			expectCapableOfInstantDDL: false,
		},
		{
			name:                      "drop column fail due to index",
			create:                    "create table t(id int, i1 int not null, i2 int not null, primary key(id), key i1_idx (i1))",
			alter:                     "alter table t drop column i1",
			expectCapableOfInstantDDL: false,
		},
		{
			name:                      "drop column fail due to multicolumn index",
			create:                    "create table t(id int, i1 int not null, i2 int not null, primary key(id), key i21_idx (i2, i1))",
			alter:                     "alter table t drop column i1",
			expectCapableOfInstantDDL: false,
		},
		{
			name:                      "drop column fail due to fulltext index in table",
			create:                    "create table t(id int, i1 int not null, name varchar(128), primary key(id), fulltext key (name))",
			alter:                     "alter table t drop column i1",
			expectCapableOfInstantDDL: false,
		},
		{
			name:                      "add two columns",
			create:                    "create table t(id int, i1 int not null, primary key(id))",
			alter:                     "alter table t add column i2 int not null after id, add column i3 int not null",
			expectCapableOfInstantDDL: true,
		},
		{
			name:                      "multiple add/drop columns",
			create:                    "create table t(id int, i1 int not null, primary key(id))",
			alter:                     "alter table t add column i2 int not null after id, add column i3 int not null, drop column i1",
			expectCapableOfInstantDDL: true,
		},
		// change/remove column default
		{
			name:                      "set a default column value",
			create:                    "create table t(id int, i1 int not null, primary key(id))",
			alter:                     "alter table t modify column i1 int not null default 0",
			expectCapableOfInstantDDL: true,
		},
		{
			name:                      "change a default column value",
			create:                    "create table t(id int, i1 int not null, primary key(id))",
			alter:                     "alter table t modify column i1 int not null default 3",
			expectCapableOfInstantDDL: true,
		},
		{
			name:                      "change a default column value on a table with FULLTEXT index",
			create:                    "create table t(id int, i1 int not null, name varchar(128), primary key(id), fulltext key (name))",
			alter:                     "alter table t modify column i1 int not null default 3",
			expectCapableOfInstantDDL: true,
		},
		{
			name:                      "change default column value to null",
			create:                    "create table t(id int, i1 int not null, primary key(id))",
			alter:                     "alter table t modify column i1 int default null",
			expectCapableOfInstantDDL: false,
		},
		{
			name:                      "fail because on top of changing the default value, the datatype is changed, too",
			create:                    "create table t(id int, i1 int not null, primary key(id))",
			alter:                     "alter table t modify column i1 bigint not null default 3",
			expectCapableOfInstantDDL: false,
		},
		{
			name:                      "set column dfault value to null",
			create:                    "create table t(id int, i1 int, primary key(id))",
			alter:                     "alter table t modify column i1 int default null",
			expectCapableOfInstantDDL: true,
		},
		// enum/set:
		{
			name:                      "change enum default value",
			create:                    "create table t(id int, c1 enum('a', 'b', 'c'), primary key(id))",
			alter:                     "alter table t modify column c1 enum('a', 'b', 'c') default 'b'",
			expectCapableOfInstantDDL: true,
		},
		{
			name:                      "enum append",
			create:                    "create table t(id int, c1 enum('a', 'b', 'c'), primary key(id))",
			alter:                     "alter table t modify column c1 enum('a', 'b', 'c', 'd')",
			expectCapableOfInstantDDL: true,
		},
		{
			name:                      "enum append with changed default",
			create:                    "create table t(id int, c1 enum('a', 'b', 'c') default 'a', primary key(id))",
			alter:                     "alter table t modify column c1 enum('a', 'b', 'c', 'd') default 'd'",
			expectCapableOfInstantDDL: true,
		},
		{
			name:                      "enum: fail insert in middle",
			create:                    "create table t(id int, c1 enum('a', 'b', 'c'), primary key(id))",
			alter:                     "alter table t modify column c1 enum('a', 'b', 'x', 'c')",
			expectCapableOfInstantDDL: false,
		},
		{
			name:                      "enum: fail change",
			create:                    "create table t(id int, c1 enum('a', 'b', 'c'), primary key(id))",
			alter:                     "alter table t modify column c1 enum('a', 'x', 'c')",
			expectCapableOfInstantDDL: false,
		},
		{
			name:                      "set: append",
			create:                    "create table t(id int, c1 set('a', 'b', 'c'), primary key(id))",
			alter:                     "alter table t modify column c1 set('a', 'b', 'c', 'd')",
			expectCapableOfInstantDDL: true,
		},
		{
			name:                      "fail set append when over threshold", // (increase from 8 to 9 values => storage goes from 1 byte to 2 bytes)
			create:                    "create table t(id int, c1 set('a', 'b', 'c', 'd', 'e', 'f', 'g', 'h'), primary key(id))",
			alter:                     "alter table t modify column c1 set('a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i')",
			expectCapableOfInstantDDL: false,
		},
	}
	for _, tcase := range tcases {
		t.Run(tcase.name, func(t *testing.T) {
			if tcase.capableOf == nil {
				tcase.capableOf = capableOf
			}
			createTable, err := parser.ParseStrictDDL(tcase.create)
			require.NoError(t, err, "failed to parse a CREATE TABLE statement from %q", tcase.create)
			createTableStmt, ok := createTable.(*sqlparser.CreateTable)
			require.True(t, ok)

			alterTable, err := parser.ParseStrictDDL(tcase.alter)
			require.NoError(t, err, "failed to parse a ALTER TABLE statement from %q", tcase.alter)
			alterTableStmt, ok := alterTable.(*sqlparser.AlterTable)
			require.True(t, ok)

			isCapableOf, err := AlterTableCapableOfInstantDDL(alterTableStmt, createTableStmt, tcase.capableOf)
			require.NoError(t, err)
			assert.Equal(t, tcase.expectCapableOfInstantDDL, isCapableOf)
		})
	}
}
