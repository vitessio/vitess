# Vitess Governance

# Overview

Vitess is a meritocratic, consensus-based community project. Anyone with an interest in the project can join the community, contribute to the project design and participate in the decision making process. This document describes how that participation takes place and how to set about earning merit within the project community.

# Roles And Responsibilities

## Users

Users are community members who have a need for the project. They are the most important members of the community and without them the project would have no purpose. Anyone can be a user; there are no special requirements.

The project asks its users to participate in the project and community as much as possible. User contributions enable the project team to ensure that they are satisfying the needs of those users. Common user contributions include (but are not limited to):

* evangelising about the project (e.g. a link on a website and word-of-mouth awareness raising)
* informing developers of strengths and weaknesses from a new user perspective
* providing moral support (a ‘thank you’ goes a long way)
* providing financial support (the software is open source, but its developers need to eat)

Users who continue to engage with the project and its community will often become more and more involved. Such users may find themselves becoming contributors, as described in the next section.

## Contributors

Contributors will be added to the [Contributors list](https://github.com/vitessio/vitess/graphs/contributors).

Contributors are community members who contribute in concrete ways to the project. Anyone can become a contributor. There is no expectation of commitment to the project, no specific skill requirements and no selection process.

In addition to their actions as users, contributors may also find themselves doing one or more of the following:

* supporting new users (existing users are often the best people to support new users)
* reporting bugs
* identifying requirements
* providing graphics and web design
* programming
* assisting with project infrastructure
* writing documentation
* fixing bugs
* adding features

Contributors engage with the project through the issue tracker, mailing list, the Slack channel, or by writing or editing documentation. They submit changes to the project itself via patches, which will be considered for inclusion in the project by existing committers (see next section). The Slack channel is the most appropriate place to ask for help when making that first contribution.

As contributors gain experience and familiarity with the project, their profile within, and commitment to, the community will increase. At some stage, they may find themselves being nominated for committership.

## Committers

Committers are community members who have shown that they are committed to the continued development of the project through ongoing engagement with the community. Committership allows contributors to more easily carry on with their project related activities by giving them direct access to the project’s resources. That is, they can make changes directly to project outputs, without having to submit changes via patches.

This does not mean that a committer is free to do what they want. In fact, committers have no more authority over the project than contributors. While committership indicates a valued member of the community who has demonstrated a healthy respect for the project’s aims and objectives, their work continues to be reviewed by the community before acceptance in an official release.

A committer is not allowed to merge their change without approval from another person. However, they are allowed to sidestep this rule under justifiable circumstances. For example:

* If a CI tool is broken, they may override the tool to still submit the change.
* Minor typos or fixes for broken tests.
* The change was approved through other means than the standard process.

Anyone can become a committer; there are no special requirements, other than to have shown a willingness and ability to participate in the project as a team player. Typically, a potential committer will need to show that they have an understanding of the project, its objectives and its strategy. They will also have provided valuable contributions to the project over a period of time.

New committers can be nominated by any existing committer. Once they have been nominated, there will be a vote by the project management committee (PMC; see below). Committer voting is one of the few activities that takes place on the project’s private management list. This is to allow PMC members to freely express their opinions about a nominee without causing embarrassment. Once the vote has been held, the aggregated voting results are published on the public mailing list. The nominee is entitled to request an explanation of any ‘no’ votes against them, regardless of the outcome of the vote. This explanation will be provided by the PMC Chair (see below) and will be anonymous and constructive in nature.

Nominees may decline their appointment as a committer. However, this is unusual, as the project does not expect any specific time or resource commitment from its community members. The intention behind the role of committer is to allow people to contribute to the project more easily, not to tie them in to the project in any formal way.

It is important to recognise that commitership is a privilege, not a right. That privilege must be earned and once earned it can be removed by the PMC for conduct inconsistent with the [Guiding Principles](https://github.com/vitessio/vitess/blob/master/GUIDING_PRINCIPLES.md) or if they drop below a level of commitment and engagement required to be a Committer, as determined by the PMC. The PMC also reserves the right to remove a person for any other reason inconsistent with the goals of Vitess.

A committer who shows an above-average level of contribution to the project, particularly with respect to its strategic direction and long-term health, may be nominated to become a member of the PMC. This role is described below.

## Project management committee

The current list of PMC members and their github handles are:

* Alain Jobart (alainjobart)
* Anthony Yeh (enisoc)
* Michael Berlin (michael-berlin)
* Michael Demmer (demmer)
* Sugu Sougoumarane (sougou)

Given that YouTube has built and supported this project, they reserve the right to appoint two seats on the PMC for a period of one year from the time this document becomes effective. After one year has passed, the normal rules of engagement apply as described below.

The project management committee consists of those individuals identified as ‘project owners’ on the development site. The PMC has additional responsibilities over and above those of a committer. These responsibilities ensure the smooth running of the project. PMC members are expected to review code contributions, participate in strategic planning, approve changes to the governance model and manage the copyrights within the project outputs.

Members of the PMC do not have significant authority over other members of the community, although it is the PMC that votes on new committers, and makes all major decisions for the future with respect to Vitess. It also makes decisions when community consensus cannot be reached. In addition, the PMC has access to the project’s private mailing list and its archives. This list is used for sensitive issues, such as votes for new committers and legal matters that cannot be discussed in public. It is never used for project management or planning.

Membership of the PMC is by invitation from the existing PMC members. A nomination will result in discussion and then a vote by the existing PMC members. PMC membership votes are subject to consensus approval of the current PMC members.

The number of PMC members should be limited to 7. This number is chosen to ensure that sufficient points of view are represented, while preserving the efficiency of the decision making process.

The PMC is responsible for maintaining the [Guiding Principles](https://github.com/vitessio/vitess/blob/master/GUIDING_PRINCIPLES.md) and the code of conduct. It is also responsible for ensuring that those rules and principles are followed.

## PMC Chair

The current PMC chair is: Sugu Sougoumarane.

The PMC Chair is a single individual, voted for by the PMC members. Once someone has been appointed Chair, they remain in that role until they choose to retire, or the PMC casts a two-thirds majority vote to remove them.

The PMC Chair has no additional authority over other members of the PMC: the role is one of coordinator and facilitator. The Chair is also expected to ensure that all governance processes are adhered to, and has the casting vote when the project fails to reach consensus.

# Support

All participants in the community are encouraged to provide support for new users within the project management infrastructure. This support is provided as a way of growing the community. Those seeking support should recognise that all support activity within the project is voluntary and is therefore provided as and when time allows. A user requiring guaranteed response times or results should therefore seek to purchase a support contract from a community member. However, for those willing to engage with the project on its own terms, and willing to help support other users, the community support channels are ideal.

# Contribution Process
Anyone can contribute to the project, regardless of their skills, as there are many ways to contribute. For instance, a contributor might be active on the Slack channel and issue tracker, or might supply patches. The various ways of contributing are described in more detail in a separate document.

The Slack channel list is the most appropriate place for a contributor to ask for help when making their first contribution.

# Decision Making Process

Decisions about the future of the project are made by the PMC. New proposals and ideas can be brought to the PMC’s attention through the Slack channel or by filing an issue. If necessary, the PMC will seek input from others to come to the final decision.

The PMC’s decision is itself governed by the project’s [Guiding Principles](https://github.com/vitessio/vitess/blob/master/GUIDING_PRINCIPLES.md), which shall be used to reach consensus. If a consensus cannot be reached, a simple majority voting process will be used to reach resolution. In case of a tie, the PMC chair has the casting vote.

# Credits
The contents of this document are based on http://oss-watch.ac.uk/resources/meritocraticgovernancemodel by Ross Gardler and Gabriel Hanganu.
