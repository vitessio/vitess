# -----------------------------------------------------------------------------
#     G N U    M A K E   S C R I P T   S P E C I F I C A T I O N
# -----------------------------------------------------------------------------
# NAME
#
#  dia.Makefile - To automate dia files rendering.
#
#
#
# REVISION HISTORY
#     09/13/2014   T.J. Yang init.
#
# USAGE
#     make [all|clean] 
#
#
#
# DESCRIPTION
# 
#
# 
# TODOs
#  1. Improve Makefile syntax.

# ---------------------------- CONSTANT DECLARATION ---------------------------

DIA     :=/usr/bin/dia
DOT     :=/usr/bin/dot
INKSCAPE:=/usr/bin/inkscape
EPSTOPDF:=`which epstopdf`
MPOST   :=/usr/bin/mpost
CONVERT :=/usr/bin/convert  

#image-sources := \
#ClusterWAN.dia \
#unix.dot  \
#pms.dot
#
#             (filter, matched pattern,filenames)
#image-dot   := $(filter %.dot,$(image-sources))
#image-dia   := $(filter %.dia,$(image-sources))

# ---------------------------- Makefile Rules  ---------------------------
# Rules
# Rule 1: Convert .dia file into .eps file 
%.eps: %.dia
	$(DIA) -t eps $< 

# Rule 2: Convert .eps file into pdf file 
%.pdf: %.eps
	$(EPSTOPDF)  $<

# Rule 2: Convert .dot file into svg file 
# dot converting to svg format
%.svg: %.dot
	$(DOT) -Tsvg -o $@ $<

# svg then converted to png format using inkscape.
# Produce 90dpi PNGs for the web.
%.png: %.svg
	$(INKSCAPE) -D -e $@ $<

#    $(patsubst pattern,replacement,text)
#For example, you might have a list of object files:
#          objects = foo.o bar.o baz.o
#To get the list of corresponding source files, you could simply write:
#          $(objects:.o=.c)
#instead of using the general form:
#          $(patsubst %.o,%.c,$(objects))
#
pdfs := $(patsubst %.dia,%.pdf,$(wildcard *.dia))
pngs := $(patsubst %.dot,%.png,$(wildcard *.dot))

all:${pdfs}  ${pngs} 

clean:  
	rm -f *.pdf *.png *~
