
lib = File.expand_path("../lib", __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require "pgslice/version"

Gem::Specification.new do |spec|
  spec.name          = "pgslice"
  spec.version       = PgSlice::VERSION
  spec.summary       = "Postgres partitioning as easy as pie"
  spec.homepage      = "https://github.com/ankane/pgslice"
  spec.license       = "MIT"

  spec.author        = "Andrew Kane"
  spec.email         = "andrew@chartkick.com"

  spec.files         = Dir["*.{md,txt}", "{lib,exe}/**/*"]
  spec.require_path  = "lib"

  spec.bindir        = "exe"
  spec.executables   = ["pgslice"]

  spec.required_ruby_version = ">= 2.2"

  spec.add_dependency "slop", ">= 4.2.0"
  spec.add_dependency "pg"

  spec.add_development_dependency "bundler"
  spec.add_development_dependency "rake"
  spec.add_development_dependency "minitest"
end
