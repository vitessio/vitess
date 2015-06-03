// Copyright 2014 Google Inc. All rights reserved.
//
// Use of this source code is governed by The MIT License.
// See the LICENSE file for details.

(function () {
  'use strict';

  var html = document.getElementsByTagName('html')[0];
  var body = document.body;
  var nav = document.getElementById('nav');
  var appbar = document.getElementById('app-bar');
  var menu = document.getElementById('menu');
  var main = document.getElementsByTagName('main')[0];

  html.className = html.className.replace('no-js', '');
  if (!('ontouchstart' in window)) {
    html.className = html.className + ' no-touch';
  }


  function closeMenu() {
    body.classList.remove('open');
    appbar.classList.remove('open');
    nav.classList.remove('open');
  }

  function toggleMenu() {
    body.classList.toggle('open');
    appbar.classList.toggle('open');
    nav.classList.toggle('open');
  }

  function handleNavClick(event) {
    if (event.target.nodeName === 'A' || event.target.nodeName === 'LI') {
      closeMenu();
    }
  }


  var previous = -1;
  function handleScroll(event) {
    var current = body.scrollTop;
    if (current >= 80 && previous < 80) {
      body.className = body.className + ' scrolled';
    } else if (current < 80 && previous >= 80) {
      body.className = body.className.replace(' scrolled', '');
    }
    previous = current;
  }

  // Menu
  main.addEventListener('click', closeMenu);
  menu.addEventListener('click', toggleMenu);
  nav.addEventListener('click', handleNavClick);
  window.addEventListener('scroll', handleScroll);
})();
