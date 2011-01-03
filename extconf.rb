require 'mkmf'

if have_library('stdc++') and have_library('hsclient')
  create_makefile('handlersocket')
end
