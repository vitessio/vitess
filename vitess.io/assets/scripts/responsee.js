/*
 * Responsee JS - v2.2 - 2015-03-08
 * http://www.myresponsee.com
 * Copyright 2015, Vision Design - graphic zoo
 * Free to use under the MIT license.
*/

jQuery(document).ready(function($) {
  //Responsee eside nav
  $('.aside-nav > ul > li ul').each(function(index, element) {
    var count = $(element).find('li').length;
    var content = '<span class="count-number"> ' + count + '</span>';
    //$(element).closest('li').children('a').append(content);
    //$(element).closest('li').children('a').toggleClass('categories-toggle');
  });
  var windowHeight = $(window).height();
  var newWindowHeight = (windowHeight - 225).toString() + 'px';
  $('.aside-nav').css('max-height', newWindowHeight);
  $(window).on('resize', function(){
    if($(this).height() <= 800){
        var newHeight = ($(this).height() - 145).toString() + 'px';
        $('.aside-nav').css('max-height', newHeight);
    }else{
        $('.aside-nav').css('max-height', ''); //delete attribute
    }
  });
/*
  $('.aside-nav > ul > li:has(ul)').addClass('aside-submenu');
  $('.aside-nav > ul ul > li:has(ul)').addClass('aside-sub-submenu'); 
  $('.aside-nav > ul > li.aside-submenu > a').click(function() {  
    $('.aside-nav ul li.aside-submenu:hover > ul').toggleClass('show-aside-ul', 'slow'); 
  }); 
  $('.aside-nav > ul ul > li.aside-sub-submenu > a').click(function() { 
    $('.aside-nav ul ul li:hover > ul').toggleClass('show-aside-ul', 'slow');
  });
  //Responsee nav   
  $(".categories-toggle").on('click', function(eve){
     eve.preventDefault(); 
     $(this).toggleClass('down');
  });
  $('.top-nav > ul > li ul').each(function(index, element) {
    var count = $(element).find('li').length;
    var content = '<span class="count-number"> ' + count + '</span>';
    $(element).closest('li').children('a').append(content);
  });
*/
  $('.top-nav > ul li:has(ul)').addClass('submenu');
  $('.top-nav > ul ul li:has(ul)').addClass('sub-submenu');
  $('.top-nav > ul ul li:has(ul)').removeClass('submenu');
/*
  $('.top-nav > ul li.submenu > a').click(function() {  
    $('.top-nav > ul li.submenu:hover > ul').toggleClass('show-ul', 'slow'); 
  }); 
  $('.top-nav > ul ul > li.sub-submenu > a').click(function() { 
    $('.top-nav ul ul li:hover > ul').toggleClass('show-ul', 'slow');   
  });
  $('.nav-text').click(function() { 
    $('.top-nav > ul').toggleClass('show-menu', 'slow');
  }); 
*/
  //Custom forms
  $(function() {
    var input = document.createElement("input");
    if (('placeholder' in input) == false) {
      $('[placeholder]').focus(function() {
        var i = $(this);
        if (i.val() == i.attr('placeholder')) {
          i.val('').removeClass('placeholder');
          if (i.hasClass('password')) {
            i.removeClass('password');
            this.type = 'password';
          }
        }
      }).blur(function() {
        var i = $(this);
        if (i.val() == '' || i.val() == i.attr('placeholder')) {
          if (this.type == 'password') {
            i.addClass('password');
            this.type = 'text';
          }
          i.addClass('placeholder').val(i.attr('placeholder'));
        }
      }).blur().parents('form').submit(function() {
        $(this).find('[placeholder]').each(function() {
          var i = $(this);
          if (i.val() == i.attr('placeholder')) i.val('');
        })
      });
    }
  });
  //Active item
  var url = window.location.href;
  $('a').filter(function() {
    return this.href == url;
  }).parent('li').addClass('active-item');
  var url = window.location.href;
  $('.aside-nav a').filter(function() {
    return this.href == url;
  }).parent('li').parent('ul').addClass('active-aside-item');
  var url = window.location.href;
  $('.aside-nav a').filter(function() {
    return this.href == url;
  }).parent('li').parent('ul').parent('li').parent('ul').addClass('active-aside-item');
  var url = window.location.href;
  $('.aside-nav a').filter(function() {
    return this.href == url;
  }).parent('li').parent('ul').parent('li').parent('ul').parent('li').parent('ul').addClass('active-aside-item');
});
