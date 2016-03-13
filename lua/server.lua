#!/bin/tarantool

local log  = require('log')
local json = require('json')
local pg   = require('pg')

box.cfg {
  listen                = 3013,
  wal_dir               = 'xlog',
  snap_dir              = 'snap',
  sophia_dir            = 'data',
  slab_alloc_arena      = 2
}

-- Добавляем привилегии
box.schema.user.grant('guest', 'read,write,execute', 'universe', '', { if_not_exists = true })

local conn = pg.connect({host = 'v-tavadov-current', port = '5433', user = 'iwtm', pass = 'xxXX1234', db = 'postgres'})


local s = box.schema.space.create('test', {if_not_exists = true})
local i = s:create_index('primary', {type = 'hash', parts = {1, 'NUM'}, if_not_exists = true})
s:truncate()

local console = require 'console'
console.listen '0.0.0.0:33015'

function get(object_id)
    local data = box.space.test:get(tonumber(object_id))

    if data == null then
        log.info("Getting object from database")
        local data = conn:execute("SELECT * FROM tarantool_test WHERE object_id = " .. object_id .. " LIMIT 1")
    else
        log.info("Getting object from cache")
    end

    return data
end

function loadWithId(object_id, saveInCache, json_data)

  if saveInCache then
    -- Сохраняем объект в кеш
    s:insert({tonumber(object_id), json_data})
  end

  -- Сохраняем объект в базу
  conn:begin()
  local result = conn:execute("INSERT INTO tarantool_test(\"data\", \"object_id\") VALUES('" .. json_data .. "', '" .. object_id .. "') RETURNING \"data\", \"id\", \"object_id\"")
  conn:commit()

  return result
end

function loadWithoutId(json_data)

  -- Сохраняем объект в базу
  conn:begin()
  local result = conn:execute("INSERT INTO tarantool_test(\"data\") VALUES('" .. json_data .. "') RETURNING \"data\", \"id\"")
  conn:commit()

  -- Сохраняем объект в кеш
  s:insert({tonumber(result[1]["id"]), json_data})

  return result
end
