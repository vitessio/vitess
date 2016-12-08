(function($){

  /**
   * Copyright 2012, Digital Fusion
   * Licensed under the MIT license.
   * http://teamdf.com/jquery-plugins/license/
   *
   * @author Sam Sehnert
   * @desc A small plugin that checks whether elements are within
   *       the user visible viewport of a web browser.
   *       only accounts for vertical position, not horizontal.
   */
  var $w = $(window);
  $.fn.visible = function(partial,hidden,direction){

    if (this.length < 1)
      return;

    var $t        = this.length > 1 ? this.eq(0) : this,
      t         = $t.get(0),
      vpWidth   = $w.width(),
      vpHeight  = $w.height(),
      direction = (direction) ? direction : 'both',
      clientSize = hidden === true ? t.offsetWidth * t.offsetHeight : true;

    if (typeof t.getBoundingClientRect === 'function'){

      // Use this native browser method, if available.
      var rec = t.getBoundingClientRect(),
        tViz = rec.top    >= 0 && rec.top    <  vpHeight,
        bViz = rec.bottom >  0 && rec.bottom <= vpHeight,
        lViz = rec.left   >= 0 && rec.left   <  vpWidth,
        rViz = rec.right  >  0 && rec.right  <= vpWidth,
        vVisible   = partial ? tViz || bViz : tViz && bViz,
        hVisible   = partial ? lViz || lViz : lViz && rViz;

      if(direction === 'both')
        return clientSize && vVisible && hVisible;
      else if(direction === 'vertical')
        return clientSize && vVisible;
      else if(direction === 'horizontal')
        return clientSize && hVisible;
    } else {

      var viewTop         = $w.scrollTop(),
        viewBottom      = viewTop + vpHeight,
        viewLeft        = $w.scrollLeft(),
        viewRight       = viewLeft + vpWidth,
        offset          = $t.offset(),
        _top            = offset.top,
        _bottom         = _top + $t.height(),
        _left           = offset.left,
        _right          = _left + $t.width(),
        compareTop      = partial === true ? _bottom : _top,
        compareBottom   = partial === true ? _top : _bottom,
        compareLeft     = partial === true ? _right : _left,
        compareRight    = partial === true ? _left : _right;

      if(direction === 'both')
        return !!clientSize && ((compareBottom <= viewBottom) && (compareTop >= viewTop)) && ((compareRight <= viewRight) && (compareLeft >= viewLeft));
      else if(direction === 'vertical')
        return !!clientSize && ((compareBottom <= viewBottom) && (compareTop >= viewTop));
      else if(direction === 'horizontal')
        return !!clientSize && ((compareRight <= viewRight) && (compareLeft >= viewLeft));
    }
  };

})(jQuery);