CREATE TABLE emp (
 EMPNO bigint NOT NULL,
 ENAME VARCHAR(10),
 JOB VARCHAR(9),
 MGR bigint,
 HIREDATE DATE,
 SAL bigint,
 COMM bigint,
 DEPTNO bigint,
 PRIMARY KEY (EMPNO)
) Engine = InnoDB
  COLLATE = utf8mb4_general_ci;

CREATE TABLE dept (
 DEPTNO bigint,
 DNAME VARCHAR(14),
 LOC VARCHAR(13),
 PRIMARY KEY (DEPTNO)
) Engine = InnoDB
  COLLATE = utf8mb4_general_ci;

CREATE TABLE uemp (
 empno bigint NOT NULL,
 ename VARCHAR(10),
 job VARCHAR(9),
 mgr bigint,
 hiredate DATE,
 sal bigint,
 comm bigint,
 deptno bigint,
 PRIMARY KEY (empno)
) Engine = InnoDB
  COLLATE = utf8mb4_general_ci;

CREATE TABLE udept (
 deptno bigint,
 dname VARCHAR(14),
 loc VARCHAR(13),
 PRIMARY KEY (deptno)
) Engine = InnoDB
  COLLATE = utf8mb4_general_ci;