require_relative "lib/pgslice/version"

Gem::Specification.new do |spec|
  spec.name          = "pgslice"
  spec.version       = PgSlice::VERSION
  spec.summary       = "Postgres partitioning as easy as pie"
  spec.homepage      = "https://github.com/ankane/pgslice"
  spec.license       = "MIT"

  spec.author        = "Andrew Kane"
  spec.email         = "andrew@ankane.org"

  spec.files         = Dir["*.{md,txt}", "{lib,exe}/**/*"]
  spec.require_path  = "lib"

  spec.bindir        = "exe"
  spec.executables   = ["pgslice"]

  spec.required_ruby_version = ">= 2.7"

  spec.add_dependency "pg", ">= 1"
  spec.add_dependency "thor"
end
