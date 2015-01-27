Gem::Specification.new { |gem|
  gem.name = 'embulk-plugin-input-sfdc-event-log-files'
  gem.version = '0.0.4'
  gem.summary = gem.description = %q{Embulk plugin for Salesforce.com Event Log Files input}
  gem.authors = 'Hiroshi Nakamura'
  gem.email = 'nahi@ruby-lang.org'
  gem.license = 'Apache 2.0'
  gem.homepage = 'https://github.com/nahi/embulk-plugin-input-sfdc-event-log-files'
  gem.files = Dir.glob('lib/**/*') + ['README.md']
  gem.test_files = gem.files.grep(/test/)
  gem.require_paths = ['lib']

  gem.add_dependency 'httpclient'
  gem.add_development_dependency 'bundler', ['~> 1.0']
  gem.add_development_dependency 'rake', ['>= 0.9.2']
}
