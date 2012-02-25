#!/usr/bin/env python
# vim: set fileencoding=utf8 shiftwidth=4 tabstop=4 textwidth=80 foldmethod=marker :
# Copyright (c) 2010, Kou Man Tong. All rights reserved.
# For licensing, see LICENSE file included in the package.

from distutils.core import setup

setup(name = "bson",
		version="0.3.2",
		packages=["bson"],
		author = "Kou Man Tong",
		author_email = "martinkou@tixxme.com",
		description = "BSON codec for Python",
		long_description = \
		"""Independent BSON codec for Python that doesn't depend on MongoDB.""",
		platforms = "Any",
		license = "BSD",
		keywords = "BSON codec",
		url = "http://github.com/martinkou/bson"
		)
