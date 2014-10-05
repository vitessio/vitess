# This makefile requires GNU make.
# What: A makefile macro function to 
# target1 target2 target3 : prerequisite1 prerequisite2  prerequisite3
#
#         command 1
#         command 2
# $< : The filename of the first prerequisite.
# $@ : The filename representing the target
# $< : The filename of the first prerequsite.
# $? : The names of all prequisites that are newer than the target, separated by spaces.
# $^ : The filnames of all the prerequisites,separted by spaces.
# $+ : Similar to $^, this i sthe names of all the prerequisites separated by spaces.
# $* : The stem of the target filname. A stem is typically a filname without its suffix.

BOOKNAME=vitess
MAINTEX=$(vitess).tex
sources := $(BOOKNAME).tex \
        99book.bib \
        99defs.tex \
        build-id.tex \
        title.tex \
        overview.tex \
        license.tex \
        Concepts.tex \
        Resharding.tex \
        Contributing.tex \
        SchemaManagement.tex \
        FAQ.tex \
        ServingGraph.tex \
        GettingStarted.tex \
        TestingOnARamDisk.tex \
        HelicopterOverview.tex \
        Tools.tex \
        Production.tex \
        Vision.tex \
        Reparenting.tex \
        Vttablet.tex \
        ReplicationGraph.tex \
        ZookeeperData.tex

# define the source files to generate images.
image-sources := \
        VTTabletModules.dia \
        VitessSpectrum.dia \
        VitessOverview-tj.dia \
        VitessOverview.dia \
	VTTabletModules.dia \
        unix.dot 

# (filter, matched pattern,filenames)
image-dot := $(filter %.dot,$(image-sources))
image-svg := $(filter %.svg,$(image-sources))
image-png := $(filter %.png,$(image-sources))
image-dia := $(filter %.dia,$(image-sources))
# what  is this ?
image-pdf  := $(image-dot:%.dot=%.pdf) $(image-svg:%.svg=%.pdf) $(image-png) 
#image-pdf  := $(image-dot:%.dot=%.pdf) 
image-html := $(image-dot:%.dot=%.png) $(image-svg:%.svg=%.png) $(image-png)
image-dia  := $(image-dia:%.dia=%.pdf)

example-prereqs := \
	/usr/bin/merge

dist-sources := \
	../html/hgicon.png \
	../html/index.html.var \
	../html/index.en.html

latex-options = \
	-interaction batchmode \
	-output-directory $(dir $(1)) \
	-jobname $(basename $(notdir $(1)))

hg = $(shell which hg)

hg-id = $(shell hg parents --template '{node|short}, dated {date|isodate},\n')

hg-version = $(shell hg version -q | \
	     sed 's,.*(version \(unknown\|[a-f0-9+]*\)),\1,')

all: pdf
#all: pdf html 
#all: html 

pdf: pdf/${BOOKNAME}.pdf

define pdf
	mkdir -p $(dir $@)
	TEXINPUTS=$(dir $<): pdflatex $(call latex-options,$@) $< || (rm -f $@; exit 1)
	cp 99book.bib $(dir $@)
	cd $(dir $@) && bibtex $(basename $(notdir $@))
	cd $(dir $@) && makeindex $(basename $(notdir $@))
	TEXINPUTS=$(dir $<): pdflatex $(call latex-options,$@) $< || (rm -f $@; exit 1)
	TEXINPUTS=$(dir $<): pdflatex $(call latex-options,$@) $< || (rm -f $@; exit 1)
	if grep 'Reference.*undefined' $(@:.pdf=.log); then exit 1; fi
endef

#pdf/${BOOKNAME}.pdf: $(sources) $(image-pdf) examples
pdf/${BOOKNAME}.pdf: $(sources) $(image-pdf)  $(image-dia)  
	$(call pdf)


html: onepage split

onepage: $(htlatex) html/onepage/xymon-rtfm.html html/onepage/xymon-rtfm.css $(image-html:%=html/onepage/%)

html/onepage/%: %
	cp $< $@

split: $(htlatex) html/split/xymon-rtfm.html html/split/xymon-rtfm.css $(image-html:%=html/split/%)

html/split/%: %
	cp $< $@

# This is a horrible hack to work around the fact that the htlatex
# command in tex4ht is itself a horrible hack.  I really don't want to
# include verbatim the big wad of TeX that is repeated in that script,
# but I've given up and run a hacked copy as htlatex.book here.

define htlatex
	mkdir -p $(dir $(1))
	cp 99book.bib $(dir $(1))
	TEXINPUTS=$(dir $(2)): ./htlatex.book $(2) "bookhtml,html4-uni,$(3)" " -cunihtf -utf8" "$(dir $(1))" "$(call latex-options,$(1))" || (rm -f $(1); exit 1)
	cd $(dir $(1)) && tex4ht -f/$(basename $(notdir $(1))) -cvalidate -cunihtf
	cd $(dir $(1)) && t4ht -f/$(basename $(notdir $(1)))
	./fixhtml.py $(dir $(1))/*.html
	rm $(dir $(1))/xymon-rtfm.css
endef

html/onepage/xymon-rtfm.html: $(sources) $(image-html) examples bookhtml.cfg
	$(call htlatex,$@,$<)

html/split/xymon-rtfm.html: $(sources) examples bookhtml.cfg
	$(call htlatex,$@,$<,2)

%.tex: %.html
	html2latex $@ $<

# Produce 90dpi PNGs for the web.

%.png: %.svg
	inkscape -D -e $@ $<

%.svg: %.dot
	dot -Tsvg -o $@ $<


# Produce eps & pdf for the book pdf
# % stem of a file name.
# $< : The filename of the first prerequsite.
%.eps: %.svg
	inkscape -E $@ $<

%.eps: %.dot
	dot -Tps -o $@ $<

%.eps: %.dia
	dia -t eps $< 
%.pdf: %.eps
	epstopdf  $<

examples: $(example-prereqs) examples/.run

examples/.run: $(example-sources:%=examples/%.run)
	touch examples/.run

examples/%.run: examples/% examples/run-example
	cd examples && ./run-example $(notdir $<)

changelog := $(wildcard ../.hg/store/00changelog.[id])
ifeq ($(changelog),)
changelog := $(wildcard ../.hg/00changelog.[id])
endif

build-id.tex: $(changelog)
	echo -n '$(hg-id)' > build-id.tex

hg_id.tex: $(hg)
	echo -n '$(hg-version)' > hg_id.tex

clean: 
	rm -rf dist html *.pdf pdf/* \
	$(image-dot:%.dot=%.pdf) \
	$(image-dot:%.dot=%.png) \
	$(image-svg:%.svg=%.pdf) \
	$(image-svg:%.svg=%.png) \
	$(image-dia:%.dia=%.pdf) \
	build-id.tex hg_id.tex \
        *.aux *.log *.idx *.dvi \
	*.lot *.lof *.jpg *.toc *.out *~ \
        }

install: pdf split $(dist-sources)
	rm -rf dist
	mkdir -p dist
	cp pdf/${BOOKNAME}.pdf dist
	cp html/split/*.{css,html,png} dist
	cp $(dist-sources) dist

rsync: install
	rsync -avz --delete dist sp.red-bean.com:public_html/xymon-rtfm

upload: all 
	sudo rsync -avz --delete pdf/*.pdf  /var/www/html/books
#	sudo rsync -avz --delete html   /var/www/html/books/
tar: 
	tar czvf /tmp/hobbit-rtfm.tgz  /home/tjyang/docs/hobbit-rtfm
	sudo rsync -avz --delete /tmp/*.tgz /var/www/html/pdf

dia:
	dia -t eps xymon-daemons.dia 
	epstopdf xymon-daemons.eps
movetmp: 
	sudo rsync -avz --delete pdf/*.pdf  ~tjyang/tmp
test: 
	pdflatex $(MAINTEX)
#perl -pi -e 's!\\hline!!' *.tex 
#perl -pi -e 's!\\\\!!' *.tex 
#perl -pi -e 's!\\begin\{document.*!!' *.tex 
#perl -pi -e 's!\\end\{doc.*!!' *.tex 
#perl -pi -e 's!\\title!\\chapter!' *.tex 
#perl -pi -e 's!\\section\*!\\section!' *.tex 
#perl -pi -e 's!\\subsection\*!\\subsection!' *.tex 
#perl -pi -e 's!\\documentclass.*!!' *.tex 
#perl -pi -e 's!\\usepackage.*!!' *.tex 
#perl -pi -e 's!\\setlength.*!!' *.tex 
#perl -pi -e 's!Man page of !!' *.tex 
#perl -pi -e 's!\\subsection!\\section!' *.tex 



