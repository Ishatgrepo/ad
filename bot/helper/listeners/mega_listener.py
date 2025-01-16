from aiofiles.os import makedirs
from asyncio import gather, Event
from secrets import token_urlsafe
from mega import MegaApi, MegaListener, MegaError, MegaRequest, MegaTransfer

from bot import LOGGER, config_dict, task_dict_lock, task_dict, non_queued_dl, queue_dict_lock
from bot.helper.telegram_helper.message_utils import sendMessage, sendStatusMessage
from bot.helper.ext_utils.bot_utils import async_to_sync, sync_to_async, is_premium_user
from bot.helper.ext_utils.links_utils import get_mega_link_type
from bot.helper.ext_utils.status_utils import get_readable_file_size
from bot.helper.mirror_utils.status_utils.mega_download_status import MegaDownloadStatus
from bot.helper.mirror_utils.status_utils.queue_status import QueueStatus
from bot.helper.ext_utils.task_manager import check_running_tasks, stop_duplicate_check
from bot.helper.listeners import tasks_listener as task


class AsyncExecutor:
    def __init__(self):
        self.continue_event = Event()

    async def do(self, function, args):
        self.continue_event.clear()
        await sync_to_async(function, *args)
        await self.continue_event.wait()

async def mega_login(
        executor,
        api,
        MAIL,
        PASS
    ):
    if MAIL and PASS:
        await sync_to_async(
            executor.do,
            api.login,
            (
                MAIL,
                PASS
            )
        )

async def mega_logout(
        executor,
        api,
        folder_api=None
    ):
    await sync_to_async(
            executor.do,
            api.logout,
            ()
        )
    if folder_api:
        await sync_to_async(
            executor.do,
            folder_api.logout,
            ()
        )
        

class MegaAppListener(MegaListener):
    _NO_EVENT_ON = (
        MegaRequest.TYPE_LOGIN,
        MegaRequest.TYPE_FETCH_NODES
    )

    def __init__(self, continue_event: Event, listener):
        self.continue_event = continue_event
        self.node = None
        self.public_node = None
        self.listener = listener
        self.is_cancelled = False
        self.error = None
        self.isFile = False
        self._bytes_transferred = 0
        self._speed = 0
        self._name = ""
        super().__init__()

    @property
    def speed(self):
        return self._speed

    @property
    def downloaded_bytes(self):
        return self._bytes_transferred

    def onRequestFinish(
            self,
            api,
            request,
            error
        ):
        if str(error).lower() != "no error":
            self.error = error.copy()
            if str(self.error).casefold() != "not found":
                LOGGER.error(f"Mega onRequestFinishError: {self.error}")
            self.continue_event.set()
            return

        request_type = request.getType()

        if request_type == MegaRequest.TYPE_LOGIN:
            api.fetchNodes()
        elif request_type == MegaRequest.TYPE_GET_PUBLIC_NODE:
            self.public_node = request.getPublicMegaNode()
            self._name = self.public_node.getName()
        elif request_type == MegaRequest.TYPE_FETCH_NODES:
            LOGGER.info("Fetching Root Node.")
            self.node = api.getRootNode()
            self._name = self.node.getName()
            LOGGER.info(f"Node Name: {self.node.getName()}")

        if (
            request_type not in self._NO_EVENT_ON
            or (
                self.node
                and "cloud drive" not in self._name.lower()
            )
        ):
            self.continue_event.set()

    def onRequestTemporaryError(
            self,
            api,
            request,
            error: MegaError
        ):
        LOGGER.error(f"Mega Request error in {error}")
        if not self.is_cancelled:
            self.is_cancelled = True
            async_to_sync(
                self.listener.onDownloadError,
                f"RequestTempError: {error.toString()}"
            )
        self.error = error.toString()
        self.continue_event.set()

    def onTransferUpdate(
            self,
            api: MegaApi,
            transfer: MegaTransfer
        ):
        if self.is_cancelled:
            api.cancelTransfer(
                transfer,
                None
            )
            self.continue_event.set()
            return
        self._speed = transfer.getSpeed()
        self._bytes_transferred = transfer.getTransferredBytes()

    def onTransferFinish(
            self,
            api: MegaApi,
            transfer: MegaTransfer,
            error
        ):
        try:
            if self.is_cancelled:
                self.continue_event.set()
            elif (
                transfer.isFinished()
                and (
                    transfer.isFolderTransfer() or
                    transfer.getFileName() == self._name
                )
            ):
                async_to_sync(self.listener.onDownloadComplete)
                self.continue_event.set()
        except Exception as e:
            LOGGER.error(e)

    def onTransferTemporaryError(
            self,
            api,
            transfer,
            error
        ):
        LOGGER.error(f"Mega download error in file {transfer.getFileName()}: {error}")
        if transfer.getState() in [
            1,
            4
        ]:
            return
        self.error = f"TransferTempError: {error.toString()} ({transfer.getFileName()})"
        if not self.is_cancelled:
            self.is_cancelled = True
            self.continue_event.set()

    async def cancel_task(self):
        self.is_cancelled = True
        await self.listener.onDownloadError("Download Canceled by user")


async def add_mega_download(listener: task.TaskListener, path : str):
    MEGA_EMAIL = config_dict['MEGA_EMAIL']
    mega_link = listener.link
    MEGA_PASSWORD = config_dict['MEGA_PASSWORD']
    path = f'{path}'
    executor = AsyncExecutor()
    api = MegaApi(None, None, None, 'WZML-X')
    folder_api = None
    mega_listener = MegaAppListener(executor.continue_event, listener)
    api.addListener(mega_listener)

    if MEGA_EMAIL and MEGA_PASSWORD:
        await executor.do(api.login, (MEGA_EMAIL, MEGA_PASSWORD))

    if get_mega_link_type(mega_link) == "file":
        await executor.do(api.getPublicNode, (mega_link,))
        node = mega_listener.public_node
        mega_listener.isFile = True
    else:
        folder_api = MegaApi(None, None, None, 'WZML-X')
        folder_api.addListener(mega_listener)
        await executor.do(folder_api.loginToFolder, (mega_link,))
        node = await sync_to_async(folder_api.authorizeNode, mega_listener.node)
    if mega_listener.error is not None:
        await sendMessage(listener.message, str(mega_listener.error))
        await executor.do(api.logout, ())
        if folder_api is not None:
            await executor.do(folder_api.logout, ())
        return

    listener.name = listener.name or node.getName()
    megadl, zuzdl, leechdl, storage = config_dict['MEGA_LIMIT'], config_dict['ZIP_UNZIP_LIMIT'], config_dict['LEECH_LIMIT'], config_dict['STORAGE_THRESHOLD']
    file, name = await stop_duplicate_check(listener, mega_listener.isFile)
    if file:
        listener.name = name
        LOGGER.info('File/folder already in Drive!')
        await gather(listener.onDownloadError('File/folder already in Drive!', file), sync_to_async(executor.do, api.logout, ()))
        if folder_api:
            await executor.do(folder_api.logout, ())
        return
        
    gid = token_urlsafe(12)
    size = api.getSize(node)
    
    msgerr = None
    megadl, zuzdl, leechdl, storage = config_dict['MEGA_LIMIT'], config_dict['ZIP_UNZIP_LIMIT'], config_dict['LEECH_LIMIT'], config_dict['STORAGE_THRESHOLD']
    if config_dict['PREMIUM_MODE'] and not is_premium_user(listener.user_id):
        mdl = zuzdl = leechdl = config_dict['NONPREMIUM_LIMIT']
        if mdl < megadl:
            megadl = mdl
    if megadl and size >= megadl * 1024**3:
        msgerr = f'Mega limit is {megadl}GB'
    if not msgerr:
        if zuzdl and any([listener.compress, listener.extract]) and size >= zuzdl * 1024**3:
            msgerr = f'Zip/Unzip limit is {zuzdl}GB'
        elif leechdl and listener.isLeech and size >= leechdl * 1024**3:
            msgerr = f'Leech limit is {leechdl}GB'
    if msgerr:
        LOGGER.info('File/folder size over the limit size!')
        await listener.onDownloadError(f'{msgerr}. File/folder size is {get_readable_file_size(size)}.')
        if folder_api:
            await executor.do(folder_api.logout, ())
        return
    
    added_to_queue, event = await check_running_tasks(listener.mid)
    if added_to_queue:
        LOGGER.info(f"Added to Queue/Download: {name}")
        async with task_dict_lock:
            task_dict[listener.mid] = QueueStatus(
                listener, size, gid, 'dl')
        await listener.onDownloadStart()
        await sendStatusMessage(listener.message)
        await event.wait()
        async with task_dict_lock:
            if listener.mid not in task_dict:
                await executor.do(api.logout, ())
                if folder_api is not None:
                    await executor.do(folder_api.logout, ())
                return
        from_queue = True
        LOGGER.info(f'Start Queued Download from Mega: {name}')
    else:
        from_queue = False

    async with task_dict_lock:
        task_dict[listener.mid] = MegaDownloadStatus(name, size, gid, mega_listener, listener.message, listener)
    async with queue_dict_lock:
        non_queued_dl.add(listener.mid)

    if from_queue:
        LOGGER.info(f'Start Queued Download from Mega: {name}')
    else:
        await listener.onDownloadStart()
        await sendStatusMessage(listener.message)
        LOGGER.info(f"Download from Mega: {name}")

    await makedirs(path, exist_ok=True)
    await executor.do(api.startDownload, (node, path, name, None, False, None))
    await executor.do(api.logout, ())
    if folder_api is not None:
        await executor.do(folder_api.logout, ())
