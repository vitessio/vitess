Please search the existing issues for relevant feature requests, and use the [reaction feature](https://blog.github.com/2016-03-10-add-reactions-to-pull-requests-issues-and-comments/) to add upvotes to pre-existing requests.

#### Feature Description

In order to improve the quality of the issue queue handling and automation purposes, we’d like to open this RFC. Tags are provided to rate both the severity and priority of issues for the Vitess maintainers’ team:


**Severity** is used to indicate how "critical" a problem is. It's a more objective rating that in theory should typically stay constant throughout the life of the issue.

**Priority** is used as a project management tool to indicate how important an issue is for the current release, milestone, and/or sprint. Because it's used for planning, the priority value is more likely than severity to be dynamic over the life of the issue.

Severity

The available tags for severity are as follows:

### Severity 1

Critical feature impact. This indicates you are unable to use the overall product resulting in a critical impact on operations. This condition requires an immediate solution. Your cluster is down or unable to serve traffic. The Vitess is unable to operate or the product caused other critical software to fail and there is no acceptable way to work around the problem. You have a significant security breach or data leak. You have data corruption, both on disk, and in the results from the cluster. 

### Severity 2

Significant feature impact. This indicates the Vitess is usable but is severely limited or degraded. Severely limited can mean that a task is unable to operate, the task caused other critical software to fail, or the task is usable but not without severe difficulty.
A serious performance degradation might fall into this category unless it was so bad as to render the system completely inoperative. This can mean that function which you were attempting to use failed to function, but a temporary workaround is available hence needs immediate attention.

### Severity 3

Some feature impact. This indicates the feature is usable but a Vitess cluster runs with minor issues/limitations. The task that you were attempting to use behaved in a manner that is incorrect and/or unexpected, or presented misleading or confusing information.
This can include documentation that was incomplete or incorrect, making it difficult to know how to use a task. This can include poor or unexplained log messages where no clear error was evident. This can include situations where some side effect is observed which does not significantly harm operations.
Documentation that causes the customer to perform some operation that damaged data (unintentional deletion, corruption, etc.) would more likely be listed as a severity 2 problem.
This can not include cases where customer data is inaccurately stored, or retrieved. Data integrity problems require a severity of 2 or higher.

### Severity 4

Minimal feature impact. This indicates the problem causes little impact on operations or that a reasonable circumvention to the problem has been implemented.
The function you were attempting to use suffers from usability quirks, requires minor documentation updates, or could be enhanced with some minor changes to the function.
This is also the place for general Help/DOC suggestions where data is NOT missing or incorrect.


**Priority**

The available tags for priority are as follows:

**P-1 Priority Critical**
Cannot ship the release/milestone until completed.
Should be addressed immediately before any lower priority items.

**_P-2 Priority High_**
Part of the "must-include" plan for the given milestone

**P-3 Priority Medium**
Highly desirable but not essential for the given milestone.

**Priority Low**
Desirable for given milestone but not essential, should be pursued if an opportunity arises to address in a safe and timely manner



#### Use Case(s)

Any relevant use-cases that you see.

