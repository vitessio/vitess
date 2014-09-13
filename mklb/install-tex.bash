# a texlive installer for ubuntu 12.04
for i in inkscape \
gnumeric \
ttf-droid \
ttf-dejavu \
ttf-sazanami-gothic \
texlive-fonts-recommended  texlive-extra-utils \
texlive-xetex \
texlive-latex-extra \
texlive-full \
latex-cjk-xcjk \
git-core \
make 
do
sudo apt-get install $i -y
done
