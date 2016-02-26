---
layout: postlist
title: Vitess Blog Posts
bodyclass: postslist
tocDepth: h2
tocHeader: Blog posts
---

{% for post in site.posts %}

  {% if site.blog.posts_before_list and forloop.index > site.blog.posts_before_list %}

{% if forloop.index0 == site.blog.posts_before_list %}
## {{ site.blog.previous_post_text }}
{% endif %}

* [{{ post.title }}]({{ post.url | prepend: site.baseurl }}), {{ post.date | date_to_string }}

  {% elsif site.excerpt_separator %}

<p style="font-size: 0.8em; margin: 0;"><b>{{ post.date | date_to_string }}{% if post.attribution %}, {{ post.attribution }}{% endif %}</b></p>

## {{ post.title }} <span>({{ post.date | date: "%b. %d %Y" }})</span>

<p>{{ post.excerpt | remove: '<p>' | remove: '</p>' }} <a href="{{ post.url | prepend: site.baseurl }}">[read more]</a></p>

{% unless forloop.last %}<hr>{% endunless %}

  {% else %}

## {{ post.title }} <span>({{ post.date | date: "%b. %d %Y" }})</span>

<div class="postdate">
Posted on {{ post.date | date: "%A, %B %d %Y" }} (<a href="{{ post.url }}">permanent link</a>)
</div>
{{ post.content }}

{% unless forloop.last %}<hr>{% endunless %}

  {% endif  %}

{% endfor %}

