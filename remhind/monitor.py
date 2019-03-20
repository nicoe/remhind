import asyncio
import logging
import os.path
import pathlib

import aionotify

ALL_EVENTS = (
    aionotify.Flags.CREATE
    | aionotify.Flags.DELETE
    | aionotify.Flags.MOVED_FROM
    | aionotify.Flags.MOVED_TO
    | aionotify.Flags.MODIFY)


async def get_watchers(config_calendars):
    watchers = []
    loop = asyncio.get_event_loop()
    for calendar in config_calendars.values():
        path = pathlib.Path(calendar['path'])
        watcher = aionotify.Watcher()
        watcher.watch(str(path.expanduser()), flags=ALL_EVENTS)
        await watcher.setup(loop)
        logging.info(f'Watcher setup for {path}')
        watchers.append(watcher)
    return watchers


async def monitor_calendars(config_calendars, calendar_store):
    watchers = await get_watchers(config_calendars)
    while True:
        done, pending = await asyncio.wait(
            [w.get_event() for w in watchers],
            return_when=asyncio.FIRST_COMPLETED)
        for task in done:
            event = await task
            path = pathlib.Path(event.alias) / event.name
            logging.debug(f'Received inotify event for {path}')
            if os.path.splitext(path)[1] != '.ics':
                continue
            if (event.flags & (
                        aionotify.Flags.CREATE | aionotify.Flags.MOVED_TO)):
                calendar_store.add_file(path)
            elif (event.flags & (
                        aionotify.Flags.DELETE | aionotify.Flags.MOVED_FROM)):
                calendar_store.remove_file(path)
            elif event.flags & aionotify.Flags.MODIFY:
                calendar_store.modify_file(path)
        for task in pending:
            task.cancel()
