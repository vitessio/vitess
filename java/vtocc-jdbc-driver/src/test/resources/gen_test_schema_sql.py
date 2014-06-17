"""Generator for SQL tests SQL schema from Vtocc test data.
"""

__author__ = 'timofeyb@google.com (Timothy Basanov)'


def PrintSection(name):
    f = open('./test/test_data/test_schema.sql', 'r')
    content = f.readlines()
    in_section = False
    for line in content:
        if line.startswith('#'):
            in_section = line.strip() == '# ' + name
            continue
        if line.strip() and in_section:
            print line.strip()


def main():
    PrintSection('clean')
    PrintSection('init')


if __name__ == '__main__':
    main()
