#!/usr/bin/env python3
# coding: utf-8

"""
trunk

Copyright (C) 2021 Tomas Hlavacek (tmshlvck@gmail.com)

This module is a part of Trunk Proxy.

This program is free software: you can redistribute it and/or modify it under
the terms of the GNU General Public License as published by the Free Software
Foundation, either version 3 of the License, or (at your option) any later
version.
This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
You should have received a copy of the GNU General Public License along with
this program. If not, see <http://www.gnu.org/licenses/>.
"""

import asyncio
import click
import yaml
import json
import logging

import asyncio
from contextlib import AsyncExitStack, asynccontextmanager
from random import randrange
from asyncio_mqtt import Client, MqttError

DEFAULT_CFG = 'trunk.yml'
BUF_LEN = 16000


#def checksum(bts):
#  s = 0
#  for b in bts:
#    s+=int(b)
#  return s


class ConnectProxy(object):
  def __init__(self, proxy_config, trunk, bchan, upchan, downchan):
    for pcfg in proxy_config:
      if bchan == pcfg['channel']:
        self.host = pcfg['connect']
        self.port = pcfg['port']
    self.upchan = upchan
    self.downchan = downchan
    self.trunk = trunk

    self.writer = None
    self.read_task = None

  async def message_handler(self, msgs):
    async for msg in msgs:
      logging.debug(f"cp_mh recv: {str(msg.payload)}")
#      print(f"cp_mh checksum: {checksum(msg.payload)}")
      self.writer.write(msg.payload)
      await self.writer.drain()

  async def close(self):
    if self.writer:
      self.writer.close()
      await self.writer.wait_closed()
      self.writer = None

    if self.read_task:
      if self.read_task.done():
        pass
      else:
        self.read_task.cancel()
        try:
          await self.read_task
        except asyncio.CancelledError:
          pass



  async def _read_handler(self):
    while not self.reader.at_eof():
      d = await self.reader.read(BUF_LEN)
#      print(f"cp_rh send checksum: {checksum(d)}")
      await self.trunk.send(self.upchan, d)


  async def start(self):
    self.reader, self.writer = await asyncio.open_connection(self.host, self.port)
    self.read_task = asyncio.create_task(self._read_handler())
    return self.read_task



class ListenProxy(object):
  def __init__(self, proxycfg, trunk):
    self.listen = proxycfg['listen']
    self.port = proxycfg['port']
    self.base_channel = proxycfg['channel']
    self.trunk = trunk

    self.server = None
    self.tasks = set()

  async def conn_handler(self, reader, writer):
    self.tasks.add(asyncio.current_task())

    class ProxyConn(object):
      def __init__(self, reader, writer):
        self.reader = reader
        self.writer = writer
        self.release_event = asyncio.Event()

      async def msg_handler(self, msgs):
        async for msg in msgs:
          logging.debug(f"lp_mh recv: {str(msg.payload)}")
#          print(f"lp_mh checksum: {checksum(d)}")
          self.writer.write(msg.payload)
          await self.writer.drain()

      async def close(self):
        if self.writer:
          self.writer.close()
          await self.writer.wait_closed()
          self.writer = None

      async def release(self):
        self.release_event.set()

      async def wait_for_release(self):
        await self.release_event.wait()


    proxyobj = None
    upchan = None
    downchan = None
    try:
      proxyobj = ProxyConn(reader, writer)
      upchan, downchan = await self.trunk.proxy_open(self.base_channel, proxyobj)
      await proxyobj.wait_for_release()

      while not reader.at_eof():
        d = await reader.read(BUF_LEN)
#        print(f"lp_ch send checksum: {checksum(d)}")
        await self.trunk.send(downchan, d)

    except asyncio.CancelledError:
      raise
    except Exception as e:
      logging.error(e, exc_info=True)
      raise
    finally:
      if proxyobj:
        await proxyobj.close()
        proxyobj = None
      if upchan and downchan:
        await self.trunk.proxy_close(upchan, downchan)
      self.tasks.discard(asyncio.current_task())


  async def start(self):
    self.server = await asyncio.start_server(self.conn_handler, self.listen, self.port)
    for s in self.server.sockets:
      addr = s.getsockname()
      logging.info(f'Serving on {addr}')

    return self.server.serve_forever()

  async def close(self):
    if self.server:
      self.server.close()
      await self.server.wait_closed()
      self.server = None
    for t in self.tasks.copy():
      if t.done():
        pass
      else:
        t.cancel()
        try:
          await t
        except asyncio.CancelledError:
          pass





class Trunk(object):
  RECONNECT_INTERVAL = 3
  KEEPALIVE = 60

  CHAN_CONTROL = 'trunk/control'

  def __init__(self, config):
    self.tasks = set()

    self.myhostname = config['hostname']
    self.hostname = config['mqtthost']
    self.port = config['mqttport']
    self.username = config['mqttuser']
    self.passwd = config['mqttpasswd']

    self.proxies = {}
    self.proxy_config = config.get('proxy', [])
    self.lastid = 1

    self.client = None

  async def send(self, topic, msg):
    logging.debug(f"mqtt send: {str(msg)}")
    await self.client.publish(topic, msg, qos=2)


  async def _control_handler(self, msggen):
    async for m in msggen:
      msg = json.loads(m.payload)
      if msg['sender'] == self.myhostname:
        continue

      logging.debug(f"mqtt recv: {str(msg)}")

      if msg['cmd'] == 'proxy_open':
        bchan = msg['base_channel']
        upchan = msg['up_channel']
        downchan = msg['down_channel']
        p = ConnectProxy(self.proxy_config, self, bchan, upchan, downchan)
        self.proxies[downchan] = p
        await self.register_channel(downchan, p.message_handler)
        self.tasks.add(await p.start())
        logging.debug("ConnectProxy started")
        await self.send(self.CHAN_CONTROL, json.dumps({'sender': self.myhostname, 'cmd': 'proxy_open_ack', 'base_channel': bchan, 'up_channel': upchan, 'down_channel': downchan}))

      elif msg['cmd'] == 'proxy_open_ack':
        bchan = msg['base_channel']
        upchan = msg['up_channel']
        downchan = msg['down_channel']
        await self.proxies[upchan].release()

      elif msg['cmd'] == 'proxy_close':
        upchan = msg['up_channel']
        downchan = msg['up_channel']
        if upchan in self.proxies:
          await self.proxies[upchan].close()
        if downchan in self.proxies:
          await self.proxies[downchan].close()

        await self.client.unsubscribe(downchan)

      else:
        logging.error(f"unknown control msg: {m.payload.decode()}")


  async def proxy_open(self, bchan, proxyobj):
    self.lastid += 1
    upchan = f"{bchan}_up{self.lastid}"
    downchan = f"{bchan}_down{self.lastid}"
    self.proxies[upchan] = proxyobj
    await self.register_channel(upchan, proxyobj.msg_handler)
    await self.send(self.CHAN_CONTROL, json.dumps({'sender': self.myhostname, 'cmd': 'proxy_open', 'base_channel': bchan, 'up_channel': upchan, 'down_channel': downchan}))
    return (upchan, downchan)


  async def proxy_close(self, upchan, downchan):
    await self.send(self.CHAN_CONTROL, json.dumps({'sender': self.myhostname, 'cmd': 'proxy_close', 'up_channel': upchan, 'down_channel': downchan}))
    await self.proxies[upchan].close()
    await self.client.unsubscribe(upchan)


  async def register_channel(self, chann, handler):
    mng = self.client.filtered_messages(chann)
    msgs = await self.exitstack.enter_async_context(mng)
    self.tasks.add(asyncio.create_task(handler(msgs)))
    await self.client.subscribe(chann)


  async def _connect(self):
    async with AsyncExitStack() as stack:
      logging.debug("Connecting to MQTT...")
      self.exitstack = stack
      stack.push_async_callback(self.cancel)

      self.client = Client(self.hostname, port=self.port, username=self.username, password=self.passwd)#, keepalive=self.KEEPALIVE)
      await stack.enter_async_context(self.client)

      await self.register_channel(self.CHAN_CONTROL, self._control_handler)

      logging.debug("MQTT connection up.")
      while True:
        done, pending = await asyncio.wait(self.tasks, return_when=asyncio.ALL_COMPLETED, timeout=10)
        for d in done:
          if d.done():
            pass
          else:
            await d
#        logging.debug(f"Reaper: done {str(done)}, pending {str(pending)}")
        if not pending:
          logging.debug("Reaper finished")
          break


  async def cancel(self):
    for t in self.tasks:
      if t.done():
        continue
      t.cancel()
      try:
        await t
      except asyncio.CancelledError:
        pass

    for p in self.proxies:
      await self.proxies[p].close()


  async def run(self):
    while True:
      try:
        await self._connect()
      except MqttError as error:
        logging.error(f'Error "{error}". Reconnecting in {self.RECONNECT_INTERVAL} seconds.')
      finally:
        await asyncio.sleep(self.RECONNECT_INTERVAL)


  async def start(self):
    t = asyncio.create_task(self.run())
    self.tasks.add(t)
    return t



async def reconfig(filename, state):
  with open(filename, 'r') as fh:
    cfgstruct = yaml.load(fh, Loader=yaml.Loader)

  # TODO: Do just diff, but now just restart everything
  for s in state:
    s.cancel()
    # stop anything in state

  t = Trunk(cfgstruct)
  state.add(await t.start())

  for p in cfgstruct.get('proxy', []):
    if p.get('listen'):
      l = ListenProxy(p, t)
      state.add(await l.start())


#  for u in cfgstruct.get('uptrunk', []):
#    uto = UpTrunk(cfgstruct['hostname'], cfgstruct['hostid'], u)
#    await uto.start()
#    state.append(uto)

#  for d in cfgstruct.get('downtrunk', []):
#    dto = DownTrunkServer(cfgstruct['hostname'], cfgstruct['hostid'], d)
#    await dto.start()
#    state.append(dto)



async def startup(cfg):
  state = set()
  await reconfig(cfg, state)

  # TODO: Add waiting for a signal / control
  while True:
    done, pending = await asyncio.wait(state, return_when=asyncio.ALL_COMPLETED, timeout=10)
    if not pending:
      break


@click.command(help="run the Trunk Proxy")
@click.option('-c', '--config', 'cfg', help=f"config YAML file (default: {DEFAULT_CFG})", type=click.Path(exists=True), default=DEFAULT_CFG)
@click.option('-d', '--debug', 'dbg', is_flag=True)
def main(cfg, dbg):
  if dbg:
    logging.basicConfig(level=logging.DEBUG)

  asyncio.run(startup(cfg))


if __name__ == '__main__':
  main()
