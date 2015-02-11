'use strict';
module.exports = function(grunt) {

  grunt.initConfig({
    pkg: grunt.file.readJSON('package.json'),
    jshint: {
      options: {
        jshintrc: '.jshintrc'
      },
      all: [
        'Gruntfile.js',
        'js/*.js',
        '!js/main.js'
      ]
    },
    watch: {
      js: {
        files: [
          '<%= jshint.all %>'
        ],
        tasks: ['jshint', 'uglify'],
        options: {
          livereload: true
        }
      },
    },
    uglify: {
      dist: {
        options: {
          banner: '/*! <%= pkg.name %> - v<%= pkg.version %> - ' + '<%= grunt.template.today("yyyy-mm-dd") %> */',
          compress: true,
          beautify: false,
          mangle: false
        },
        files: {
          'js/main.js': [
            'js/plugins/*.js',
            'js/_*.js'
          ]
        }
      }
    },
    imagemin: {
      dist: {
        options: {
          optimizationLevel: 7,
          progressive: true
        },
        files: [{
          expand: true,
          cwd: 'images/',
          src: '{,*/}*.{png,jpg,jpeg}',
          dest: 'images/'
        }]
      }
    },
    imgcompress: {
      dist: {
        options: {
          optimizationLevel: 7,
          progressive: true
        },
        files: [{
          expand: true,
          cwd: 'images/',
          src: '{,*/}*.{png,jpg,jpeg}',
          dest: 'images/'
        }]
      }
    },
    svgmin: {
      dist: {
        files: [{
          expand: true,
          cwd: 'images/',
          src: '{,*/}*.svg',
          dest: 'images/'
        }]
      }
    },
  });

  // Load tasks
  grunt.loadNpmTasks('grunt-contrib-jshint');
  grunt.loadNpmTasks('grunt-contrib-watch');
  grunt.loadNpmTasks('grunt-contrib-uglify');
  grunt.loadNpmTasks('grunt-newer');
  grunt.loadNpmTasks('grunt-contrib-imagemin');
  grunt.loadNpmTasks('grunt-svgmin');
  grunt.loadNpmTasks('grunt-imgcompress');

  // Register tasks
  grunt.registerTask('scripts', ['watch', 'uglify']);
  grunt.registerTask('images', ['newer:imgcompress', 'newer:svgmin']);
};