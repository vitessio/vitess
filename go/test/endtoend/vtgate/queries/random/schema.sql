CREATE TABLE emp (
 EMPNO bigint NOT NULL,
 ENAME VARCHAR(10),
 JOB VARCHAR(9),
 MGR bigint,
 HIREDATE DATE,
 SAL bigint,
 COMM bigint,
 DEPTNO bigint,
 GRADE ENUM('A', 'B', 'C', 'D', 'E'),
 SKILLS SET('sql', 'go', 'java', 'python'),
 META JSON,
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