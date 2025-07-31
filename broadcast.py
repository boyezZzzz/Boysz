import asyncio
import random
import datetime as dt
import time as time_module
import traceback

from gc import get_objects
from asyncio import sleep
from pyrogram.raw.functions.messages import DeleteHistory, StartBot
from pyrogram.errors.exceptions import *
from pyrogram.errors.exceptions.not_acceptable_406 import ChannelPrivate
from pyrogram.enums import ChatType
from pyrogram.types import InputTextMessageContent, InlineQueryResultArticle

from PyroUbot import *

__MODULE__ = "ʙʀᴏᴀᴅᴄᴀꜱᴛ"
__HELP__ = """
<blockquote><b>Bantuan Broadcast

perintah : <code>{0}gikes</code>

type : all , users , group

all untuk semua , users untuk user, group untuk group

perintah : <code>{0}stopg</code>
    untuk menghentikan proses gikes yang sedang berlangsung

perintah : <code>{0}bcfd</code> or <code>{0}cfd</code>
    mengirim pesan siaran secara forward

perintah : <code>{0}send</code>
    mengirim pesan ke user/group/channel

perintah : <code>{0}autobc</code>
    mengirim pesan siaran secara otomatis

Tutor menjalankan autobc new: <a href=https://t.me/InfoKingzUserbot/28>ᴄʟɪᴄᴋ ᴋɪɴɢᴢ</a>
Tutor autobc timer: <a href=https://t.me/InfoKingzUserbot/41>ᴄʟɪᴄᴋ ᴋɪɴɢᴢ</a>

query:
    |on/off |text |delay |remove |limit |timer |timer_off |timer_status</b></blockquote>
"""

# Menambahkan variabel global untuk menyimpan fitur-fitur yang sedang aktif
AG = []  # Auto Gcast active user IDs
LT = []  # Limit check active user IDs
timer_checker_users = []  # Timer checker active user IDs
gcast_progress = []  # Untuk tracking proses gcast

# Dictionary untuk melacak task yang sedang berjalan untuk setiap user
# Ini untuk mencegah duplikasi task autobc
active_tasks = {}  # {user_id: {"autobc": task_obj, "limit_check": task_obj, "timer": task_obj}}

# Lock untuk mengamankan operasi pada list dan dictionary global
AG_lock = asyncio.Lock()
LT_lock = asyncio.Lock()
timer_lock = asyncio.Lock()
task_lock = asyncio.Lock()

# Fungsi untuk inisialisasi fitur yang sebelumnya aktif
async def init_active_features():
    try:
        print("[DEBUG] Memulai inisialisasi fitur aktif...")
        # Cek dan aktifkan kembali fitur autobc yang sebelumnya aktif
        for client in ubot._ubot:
            try:
                # Pastikan user ID belum ada di active_tasks untuk mencegah duplikasi
                async with task_lock:
                    if client.me.id not in active_tasks:
                        active_tasks[client.me.id] = {}

                # Gunakan lock untuk menghindari race condition
                async with AG_lock:
                    autobc_active = await get_vars(client.me.id, "AUTO_GCAST_ACTIVE")
                    
                    if autobc_active and client.me.id not in AG:
                        print(f"[INFO] Mengaktifkan kembali autobc untuk user {client.me.id}")
                        # Dapatkan timestamp broadcast terakhir
                        last_broadcast_data = await get_vars(client.me.id, "LAST_BROADCAST_INFO")
                        current_time = time_module.time()
                        
                        # Dapatkan setting delay
                        delay_minutes = await get_vars(client.me.id, "DELAY_GCAST") or 1
                        delay_seconds = int(delay_minutes) * 60
                        
                        initial_delay = 0
                        
                        # Jika ada informasi broadcast terakhir, hitung sisa delay
                        if last_broadcast_data and isinstance(last_broadcast_data, dict):
                            last_broadcast_time = last_broadcast_data.get("timestamp", 0)
                            last_broadcast_delay = last_broadcast_data.get("delay", delay_minutes)
                            
                            # Konversi delay menit ke detik untuk perhitungan
                            last_delay_seconds = int(last_broadcast_delay) * 60
                            
                            # Hitung berapa lama waktu berlalu sejak broadcast terakhir
                            elapsed_time = current_time - last_broadcast_time
                            
                            # Jika waktu yang berlalu kurang dari delay yang ditentukan,
                            # tunggu sisa waktunya sebelum memulai broadcast lagi
                            if elapsed_time < last_delay_seconds:
                                remaining_delay = last_delay_seconds - elapsed_time
                                initial_delay = remaining_delay
                                
                                # Log informasi delay untuk debugging
                                print(f"[DEBUG] User {client.me.id}: Melanjutkan autobc dengan sisa delay {remaining_delay/60:.2f} menit")
                                
                                # Kirim notifikasi ke user tentang sisa delay
                                try:
                                    brhsl = await EMO.BERHASIL(client)
                                    await client.send_message(
                                        client.me.id,
                                        f"{brhsl} <emoji id=4942823933909926751>👋</emoji>Halo Kingz auto broadcast dilanjutkan dengan sisa delay {remaining_delay/60:.2f} menit"
                                    )
                                except Exception as e:
                                    print(f"[ERROR] Gagal mengirim notifikasi: {e}")
                            else:
                                print(f"[DEBUG] User {client.me.id}: Delay sudah berlalu, akan memulai broadcast segera")
                        else:
                            print(f"[DEBUG] User {client.me.id}: Tidak ada data broadcast terakhir")
                        
                        # Tambahkan user ke list active autobc users
                        AG.append(client.me.id)
                        
                        # Aktifkan kembali autobc dengan initial delay (sisa delay sebelumnya)
                        print(f"[INFO] Menjalankan autobc_task untuk user {client.me.id} dengan initial delay {initial_delay/60:.2f} menit")
                        
                        # Buat task baru dan simpan referensinya untuk mencegah duplikasi
                        async with task_lock:
                            # Periksa dan batalkan task yang mungkin sudah berjalan
                            if "autobc" in active_tasks[client.me.id]:
                                old_task = active_tasks[client.me.id]["autobc"]
                                if not old_task.done() and not old_task.cancelled():
                                    print(f"[WARNING] Task autobc untuk {client.me.id} sudah berjalan, membatalkan...")
                                    old_task.cancel()
                            
                            # Buat task baru
                            new_task = asyncio.create_task(autobc_task(client, initial_delay))
                            active_tasks[client.me.id]["autobc"] = new_task
                    else:
                        if autobc_active:
                            print(f"[INFO] User {client.me.id} sudah aktif dalam list AG")
                        else:
                            print(f"[INFO] User {client.me.id} tidak memiliki autobc aktif")
            except Exception as e:
                print(f"[ERROR] Error saat inisialisasi autobc untuk user {client.me.id}: {e}")
                print(f"[DEBUG] Stack trace: {traceback.format_exc()}")
                
            try:
                async with LT_lock:
                    limit_active = await get_vars(client.me.id, "AUTO_LIMIT_CHECK_ACTIVE")
                    if limit_active and client.me.id not in LT:
                        print(f"[INFO] Mengaktifkan kembali limit check untuk user {client.me.id}")
                        LT.append(client.me.id)
                        
                        # Buat task baru dan simpan referensinya
                        async with task_lock:
                            # Periksa dan batalkan task yang mungkin sudah berjalan
                            if "limit_check" in active_tasks[client.me.id]:
                                old_task = active_tasks[client.me.id]["limit_check"]
                                if not old_task.done() and not old_task.cancelled():
                                    print(f"[WARNING] Task limit_check untuk {client.me.id} sudah berjalan, membatalkan...")
                                    old_task.cancel()
                            
                            # Buat task baru
                            new_task = asyncio.create_task(limit_check_task(client))
                            active_tasks[client.me.id]["limit_check"] = new_task
            except Exception as e:
                print(f"[ERROR] Error saat inisialisasi limit check untuk user {client.me.id}: {e}")
                print(f"[DEBUG] Stack trace: {traceback.format_exc()}")
                
            try:
                async with timer_lock:
                    # Cek dan aktifkan kembali timer checker untuk user yang memiliki timer aktif
                    timer_settings = await get_vars(client.me.id, "AUTOBC_TIMER")
                    if timer_settings and timer_settings.get("enabled") and client.me.id not in timer_checker_users:
                        print(f"[INFO] Mengaktifkan kembali timer checker untuk user {client.me.id}")
                        timer_checker_users.append(client.me.id)
                        
                        # Buat task baru dan simpan referensinya
                        async with task_lock:
                            # Periksa dan batalkan task yang mungkin sudah berjalan
                            if "timer" in active_tasks[client.me.id]:
                                old_task = active_tasks[client.me.id]["timer"]
                                if not old_task.done() and not old_task.cancelled():
                                    print(f"[WARNING] Task timer untuk {client.me.id} sudah berjalan, membatalkan...")
                                    old_task.cancel()
                            
                            # Buat task baru
                            new_task = asyncio.create_task(timer_checker_task(client))
                            active_tasks[client.me.id]["timer"] = new_task
            except Exception as e:
                print(f"[ERROR] Error saat inisialisasi timer checker untuk user {client.me.id}: {e}")
                print(f"[DEBUG] Stack trace: {traceback.format_exc()}")
                
        print("[INFO] Inisialisasi fitur aktif selesai")
    except Exception as e:
        print(f"[CRITICAL] Error fatal pada init_active_features: {e}")
        print(f"[DEBUG] Stack trace: {traceback.format_exc()}")

# Jalankan inisialisasi saat bot dimulai
asyncio.create_task(init_active_features())

# PERBAIKAN: Fungsi is_time_between yang direvisi
def is_time_between(start_time, end_time, current_time):
    """
    Memeriksa apakah current_time berada di antara start_time dan end_time.
    Semua waktu harus dalam format "HH:MM".
    """
    # Validasi format untuk memastikan input benar
    if not all(len(t.split(":")) == 2 for t in [start_time, end_time, current_time]):
        print(f"[WARNING] Format waktu tidak valid: {start_time}, {end_time}, {current_time}")
        return False
        
    try:
        # PERBAIKAN: Gunakan alias dt untuk menghindari nested reference
        start = dt.datetime.strptime(start_time, "%H:%M").time()
        end = dt.datetime.strptime(end_time, "%H:%M").time()  # Fixed typo in format string
        current = dt.datetime.strptime(current_time, "%H:%M").time()
        
        # Menangani kasus di mana waktu akhir ada di hari berikutnya
        if end < start:
            return current >= start or current <= end
        else:
            return start <= current <= end
    except ValueError as e:
        print(f"[ERROR] Error dalam format waktu: {e}")
        return False

# Fungsi untuk memeriksa timer dan mengaktifkan/menonaktifkan autobc secara otomatis
async def timer_checker_task(client):
    task_id = None
    try:
        print(f"[INFO] Memulai timer_checker_task untuk user {client.me.id}")
        brhsl = await EMO.BERHASIL(client)
        ggl = await EMO.GAGAL(client)
        
        # Task ID untuk mencegah duplikasi
        task_id = f"timer_{client.me.id}_{int(time_module.time())}"
        print(f"[DEBUG] Timer task started with ID: {task_id}")
        
        while client.me.id in timer_checker_users:
            try:
                timer_settings = await get_vars(client.me.id, "AUTOBC_TIMER") or {}
                
                if timer_settings.get("enabled"):
                    start_time = timer_settings.get("start_time")
                    end_time = timer_settings.get("end_time")
                    
                    if start_time and end_time:
                        # Dapatkan waktu saat ini
                        now = dt.datetime.now().strftime("%H:%M")
                        
                        # Periksa apakah waktu saat ini berada dalam waktu siaran terjadwal
                        should_be_active = is_time_between(start_time, end_time, now)
                        
                        # Dapatkan status autobc dari database
                        autobc_active = await get_vars(client.me.id, "AUTO_GCAST_ACTIVE")
                        
                        # Jika seharusnya aktif tapi tidak aktif saat ini
                        if should_be_active and not autobc_active:
                            print(f"[INFO] Mengaktifkan autobc via timer untuk user {client.me.id} pada {now}")
                            # Pastikan kita memiliki pesan auto text
                            auto_text_vars = await get_vars(client.me.id, "AUTO_TEXT")
                            if auto_text_vars:
                                async with AG_lock:
                                    # Set flag bahwa autobc aktif
                                    await set_vars(client.me.id, "AUTO_GCAST_ACTIVE", True)
                                    
                                    # Tambahkan ke list aktif jika belum ada
                                    if client.me.id not in AG:
                                        AG.append(client.me.id)
                                
                                # Catat timestamp mulai
                                current_time = time_module.time()
                                broadcast_info = {
                                    "timestamp": current_time,
                                    "delay": await get_vars(client.me.id, "DELAY_GCAST") or 1,
                                    "putaran": 0
                                }
                                await set_vars(client.me.id, "LAST_BROADCAST_INFO", broadcast_info)
                                
                                # Jalankan autobc di background
                                async with task_lock:
                                    # Periksa dan batalkan task yang mungkin sudah berjalan
                                    if "autobc" in active_tasks.get(client.me.id, {}):
                                        old_task = active_tasks[client.me.id]["autobc"]
                                        if not old_task.done() and not old_task.cancelled():
                                            print(f"[WARNING] Task autobc untuk {client.me.id} sudah berjalan, membatalkan...")
                                            old_task.cancel()
                                    
                                    # Buat task baru
                                    new_task = asyncio.create_task(autobc_task(client))
                                    if client.me.id not in active_tasks:
                                        active_tasks[client.me.id] = {}
                                    active_tasks[client.me.id]["autobc"] = new_task
                                
                                # Catat bahwa autobc dimulai oleh timer
                                try:
                                    await client.send_message(
                                        client.me.id, 
                                        f"{brhsl}Auto gcast diaktifkan oleh timer pada {now}"
                                    )
                                except Exception as e:
                                    print(f"[ERROR] Gagal mengirim notifikasi timer on: {e}")
                        
                        # Jika seharusnya tidak aktif tapi aktif saat ini
                        elif not should_be_active and autobc_active:
                            print(f"[INFO] Menonaktifkan autobc via timer untuk user {client.me.id} pada {now}")
                            async with AG_lock:
                                # Set flag bahwa autobc tidak aktif
                                await set_vars(client.me.id, "AUTO_GCAST_ACTIVE", False)
                                
                                # Hapus dari list aktif jika ada
                                if client.me.id in AG:
                                    AG.remove(client.me.id)
                            
                            # Batalkan task autobc yang berjalan
                            async with task_lock:
                                if "autobc" in active_tasks.get(client.me.id, {}):
                                    old_task = active_tasks[client.me.id]["autobc"]
                                    if not old_task.done() and not old_task.cancelled():
                                        print(f"[INFO] Membatalkan task autobc untuk {client.me.id} via timer")
                                        old_task.cancel()
                            
                            # Catat bahwa autobc dihentikan oleh timer
                            try:
                                await client.send_message(
                                    client.me.id, 
                                    f"{brhsl}Auto gcast dinonaktifkan oleh timer pada {now}"
                                )
                            except Exception as e:
                                print(f"[ERROR] Gagal mengirim notifikasi timer off: {e}")
                
                # Periksa setiap menit
                await asyncio.sleep(60)
            except Exception as e:
                print(f"[ERROR] Error dalam siklus timer_checker untuk user {client.me.id}: {e}")
                print(f"[DEBUG] Stack trace: {traceback.format_exc()}")
                # Tunggu sebentar sebelum mencoba lagi
                await asyncio.sleep(60)
    except asyncio.CancelledError:
        print(f"[INFO] Task timer_checker untuk user {client.me.id} dibatalkan (ID: {task_id})")
        # Hapus dari daftar timer aktif
        async with timer_lock:
            if client.me.id in timer_checker_users:
                timer_checker_users.remove(client.me.id)
        # Hapus referensi task
        async with task_lock:
            if client.me.id in active_tasks and "timer" in active_tasks[client.me.id]:
                active_tasks[client.me.id].pop("timer", None)
        raise  # Re-raise to complete cancellation
    except Exception as e:
        print(f"[CRITICAL] Error fatal pada timer_checker_task: {e}")
        print(f"[DEBUG] Stack trace: {traceback.format_exc()}")
        
        async with timer_lock:
            if client.me.id in timer_checker_users:
                timer_checker_users.remove(client.me.id)
        
        # Hapus referensi task
        async with task_lock:
            if client.me.id in active_tasks and "timer" in active_tasks[client.me.id]:
                active_tasks[client.me.id].pop("timer", None)
        
        # Coba kirim notifikasi kegagalan ke user
        try:
            ggl = await EMO.GAGAL(client)
            await client.send_message(
                client.me.id,
                f"{ggl}Timer checker mengalami kesalahan fatal dan dihentikan. Error: {str(e)}"
            )
        except Exception:
            pass

# Fungsi utama autobc_task yang telah dioptimasi
async def autobc_task(client, initial_delay=0):
    task_id = None
    try:
        task_id = f"autobc_{client.me.id}_{int(time_module.time())}"
        print(f"[INFO] Memulai autobc_task untuk user {client.me.id} dengan initial delay {initial_delay} detik (ID: {task_id})")
        
        # Tunggu sisa delay jika ada
        if initial_delay > 0:
            print(f"[INFO] Menunggu initial delay {initial_delay/60:.2f} menit sebelum broadcast pertama")
            await asyncio.sleep(initial_delay)
            
        done = 0
        while True:
            try:
                # Verifikasi status aktif dari database untuk memastikan konsistensi
                autobc_active = await get_vars(client.me.id, "AUTO_GCAST_ACTIVE")
                
                # Cek status sebelum melanjutkan
                async with AG_lock:
                    is_still_active = autobc_active and client.me.id in AG
                
                if not is_still_active:
                    print(f"[INFO] Autobc dinonaktifkan untuk user {client.me.id}")
                    # Pastikan flag konsisten di database
                    await set_vars(client.me.id, "AUTO_GCAST_ACTIVE", False)
                    
                    # Pastikan tidak ada di list aktif
                    async with AG_lock:
                        if client.me.id in AG:
                            AG.remove(client.me.id)
                    
                    # Hapus referensi task
                    async with task_lock:
                        if client.me.id in active_tasks and "autobc" in active_tasks[client.me.id]:
                            active_tasks[client.me.id].pop("autobc", None)
                    break
                
                # Ambil konfigurasi untuk broadcast
                delay = await get_vars(client.me.id, "DELAY_GCAST") or 1
                auto_messages = await get_vars(client.me.id, "AUTO_TEXT") or []
                # Get broadcast mode (copy or forward)
                forward_mode = await get_vars(client.me.id, "AUTOBC_FORWARD_MODE") or False
                
                if not auto_messages:
                    print(f"[WARNING] Tidak ada pesan autobc untuk user {client.me.id}, menghentikan autobc")
                    async with AG_lock:
                        if client.me.id in AG:
                            AG.remove(client.me.id)
                    await set_vars(client.me.id, "AUTO_GCAST_ACTIVE", False)
                    
                    # Hapus referensi task
                    async with task_lock:
                        if client.me.id in active_tasks and "autobc" in active_tasks[client.me.id]:
                            active_tasks[client.me.id].pop("autobc", None)
                    
                    try:
                        ggl = await EMO.GAGAL(client)
                        await client.send_message(
                            client.me.id,
                            f"{ggl}auto_gcast: Tidak ada pesan yang tersimpan, auto_gcast dihentikan"
                        )
                    except Exception as e:
                        print(f"[ERROR] Gagal mengirim notifikasi tidak ada pesan: {e}")
                    break
                
                # Select a random message from the stored messages
                msg_data = random.choice(auto_messages)
                blacklist = await get_list_from_vars(client.me.id, "BL_ID")
                bcs = await EMO.BROADCAST(client)
                brhsl = await EMO.BERHASIL(client)
                mng = await EMO.MENUNGGU(client)
                ggl = await EMO.GAGAL(client)
                
                # Prepare the message to broadcast
                source_msg = None
                text_content = None
                missing_message = False
                
                # Penanganan berbagai format pesan
                if isinstance(msg_data, dict):  # New format
                    if msg_data.get("type") == "message_ref":
                        # Try to get the referenced message
                        try:
                            chat_id = msg_data.get("chat_id")
                            message_id = msg_data.get("message_id")
                            print(f"[DEBUG] Mengambil pesan referensi dari chat {chat_id}, message {message_id}")
                            source_msg = await client.get_messages(chat_id, message_id)
                            if not source_msg:
                                missing_message = True
                                print(f"[WARNING] Pesan referensi tidak ditemukan (ID: {message_id})")
                        except Exception as e:
                            missing_message = True
                            print(f"[ERROR] Gagal mengambil pesan referensi: {e}")
                            print(f"[DEBUG] Stack trace: {traceback.format_exc()}")
                    elif msg_data.get("type") == "text":
                        text_content = msg_data.get("content")
                        print(f"[DEBUG] Menggunakan konten teks untuk broadcast")
                else:  # Legacy format (just text)
                    text_content = msg_data
                    print(f"[DEBUG] Menggunakan format teks legacy untuk broadcast")
                
                # Penanganan pesan yang sudah tidak ada
                if missing_message:
                    print(f"[INFO] Menghapus pesan yang tidak ditemukan dari daftar autobc")
                    # Remove the missing message from auto_messages
                    auto_messages = [m for m in auto_messages if not (
                        isinstance(m, dict) and
                        m.get("type") == "message_ref" and
                        m.get("chat_id") == msg_data.get("chat_id") and
                        m.get("message_id") == msg_data.get("message_id")
                    )]
                    await set_vars(client.me.id, "AUTO_TEXT", auto_messages)
                    
                    # Notify user that a message was removed
                    try:
                        await client.send_message(
                            client.me.id,
                            f"{ggl}auto_gcast: Pesan yang tersimpan tidak ditemukan dan telah dihapus dari daftar"
                        )
                    except Exception as e:
                        print(f"[ERROR] Gagal mengirim notifikasi pesan dihapus: {e}")
                        print(f"[DEBUG] Stack trace: {traceback.format_exc()}")
                    
                    # If no messages left, stop auto_gcast
                    if not auto_messages:
                        print(f"[WARNING] Tidak ada pesan tersisa, menghentikan autobc")
                        async with AG_lock:
                            if client.me.id in AG:
                                AG.remove(client.me.id)
                        await set_vars(client.me.id, "AUTO_GCAST_ACTIVE", False)
                        
                        # Hapus referensi task
                        async with task_lock:
                            if client.me.id in active_tasks and "autobc" in active_tasks[client.me.id]:
                                active_tasks[client.me.id].pop("autobc", None)
                        
                        try:
                            await client.send_message(
                                client.me.id,
                                f"{ggl}auto_gcast: Semua pesan telah dihapus, auto_gcast dihentikan"
                            )
                        except Exception as e:
                            print(f"[ERROR] Gagal mengirim notifikasi semua pesan dihapus: {e}")
                            print(f"[DEBUG] Stack trace: {traceback.format_exc()}")
                        break
                    
                    # Skip this iteration and try with another message next time
                    print(f"[INFO] Menunggu 30 detik sebelum mencoba broadcast dengan pesan lain")
                    await asyncio.sleep(30)
                    continue

                # Proses broadcast ke semua grup
                group = 0
                print(f"[INFO] Memulai proses broadcast ke grup")
                
                async for dialog in client.get_dialogs():
                    # Verifikasi ulang status aktif pada setiap iterasi grup untuk memastikan konsistensi
                    async with AG_lock:
                        autobc_active = await get_vars(client.me.id, "AUTO_GCAST_ACTIVE")
                        if not autobc_active or client.me.id not in AG:
                            print(f"[INFO] Autobc dinonaktifkan selama proses broadcast ke grup")
                            break
                    
                    if (
                        dialog.chat.type in (ChatType.GROUP, ChatType.SUPERGROUP)
                        and dialog.chat.id not in blacklist
                        and dialog.chat.id not in BLACKLIST_CHAT
                    ):
                        try:
                            await asyncio.sleep(1)  # Rate limiting untuk menghindari flood
                            if source_msg:
                                if forward_mode:
                                    # Use forward if in forward mode
                                    await source_msg.forward(dialog.chat.id)
                                else:
                                    # Use copy to preserve premium emoji (default)
                                    await source_msg.copy(dialog.chat.id)
                            else:
                                await client.send_message(dialog.chat.id, text_content)
                            group += 1
                        except FloodWait as e:
                            print(f"[WARNING] FloodWait terjadi: menunggu {e.value} detik")
                            await asyncio.sleep(e.value)
                            # Coba lagi setelah menunggu
                            try:
                                if source_msg:
                                    if forward_mode:
                                        await source_msg.forward(dialog.chat.id)
                                    else:
                                        await source_msg.copy(dialog.chat.id)
                                else:
                                    await client.send_message(dialog.chat.id, text_content)
                                group += 1
                            except Exception as e2:
                                print(f"[ERROR] Gagal mengirim pesan setelah FloodWait: {e2}")
                                print(f"[DEBUG] Stack trace: {traceback.format_exc()}")
                        except Exception as e:
                            # Catat error tapi lanjutkan ke grup berikutnya
                            print(f"[ERROR] Gagal mengirim ke grup {dialog.chat.id}: {e}")
                            print(f"[DEBUG] Stack trace: {traceback.format_exc()}")

                # Cek kembali status aktif setelah selesai broadcast
                async with AG_lock:
                    autobc_active = await get_vars(client.me.id, "AUTO_GCAST_ACTIVE")
                    if not autobc_active or client.me.id not in AG:
                        print(f"[INFO] Autobc dinonaktifkan setelah selesai broadcast ke grup")
                        
                        # Hapus referensi task
                        async with task_lock:
                            if client.me.id in active_tasks and "autobc" in active_tasks[client.me.id]:
                                active_tasks[client.me.id].pop("autobc", None)
                        
                        break

                done += 1
                print(f"[INFO] Broadcast selesai untuk putaran ke-{done}, berhasil ke {group} grup")
                
                # Simpan timestamp broadcast terakhir bersama delay-nya
                current_time = time_module.time()
                broadcast_info = {
                    "timestamp": current_time,
                    "delay": delay,
                    "putaran": done
                }
                await set_vars(client.me.id, "LAST_BROADCAST_INFO", broadcast_info)
                
                # Kirim pesan status ke private chat user
                try:
                    mode_text = "FORWARD" if forward_mode else "COPY"
                    await client.send_message(client.me.id, f"""
{bcs}auto_gcaꜱt done (Mode: {mode_text})
putaran {done}
{brhsl}ꜱucceꜱ {group} group
{mng}wait {delay} minuteꜱ
""")
                except Exception as e:
                    print(f"[ERROR] Gagal mengirim pesan status: {e}")
                    print(f"[DEBUG] Stack trace: {traceback.format_exc()}")
                
                # Tunggu sesuai delay yang dikonfigurasi - gunakan exat_seconds untuk akurasi
                delay_seconds = int(60 * int(delay))
                print(f"[INFO] Menunggu {delay} menit ({delay_seconds} detik) untuk broadcast berikutnya")
                
                # Gunakan waktu mulai yang tepat untuk perhitungan delay yang lebih akurat
                start_sleep_time = time_module.time()
                
                # Gunakan loop sleep kecil agar dapat merespons pembatalan dengan lebih cepat
                remaining_sleep = delay_seconds
                while remaining_sleep > 0:
                    # Cek status aktif secara berkala
                    async with AG_lock:
                        autobc_active = await get_vars(client.me.id, "AUTO_GCAST_ACTIVE")
                        if not autobc_active or client.me.id not in AG:
                            print(f"[INFO] Autobc dinonaktifkan selama menunggu delay")
                            break
                    
                    # Sleep interval kecil (5 detik) untuk responsif terhadap pembatalan
                    sleep_chunk = min(5, remaining_sleep)
                    await asyncio.sleep(sleep_chunk)
                    remaining_sleep -= sleep_chunk
                
                # Setelah keluar dari loop sleep, periksa lagi apakah autobc masih aktif
                async with AG_lock:
                    autobc_active = await get_vars(client.me.id, "AUTO_GCAST_ACTIVE")
                    if not autobc_active or client.me.id not in AG:
                        print(f"[INFO] Autobc dinonaktifkan setelah delay sleep")
                        
                        # Hapus referensi task
                        async with task_lock:
                            if client.me.id in active_tasks and "autobc" in active_tasks[client.me.id]:
                                active_tasks[client.me.id].pop("autobc", None)
                        
                        break
                
                # Log waktu sleep yang sebenarnya
                actual_sleep_time = time_module.time() - start_sleep_time
                print(f"[DEBUG] Waktu delay sebenarnya: {actual_sleep_time:.2f} detik (target: {delay_seconds} detik)")
            
            except asyncio.CancelledError:
                print(f"[INFO] Task autobc untuk user {client.me.id} dibatalkan (ID: {task_id})")
                raise  # Re-raise untuk menyelesaikan pembatalan
            except Exception as e:
                print(f"[ERROR] Error dalam siklus broadcast: {e}")
                print(f"[DEBUG] Stack trace: {traceback.format_exc()}")
                # Jangan keluar dari loop, tunggu sebentar lalu coba lagi
                await asyncio.sleep(300)  # Tunggu 5 menit sebelum mencoba lagi
    
    except asyncio.CancelledError:
        print(f"[INFO] Task autobc untuk user {client.me.id} dibatalkan (ID: {task_id})")
        # Hapus referensi task jika dibatalkan
        async with task_lock:
            if client.me.id in active_tasks and "autobc" in active_tasks[client.me.id]:
                active_tasks[client.me.id].pop("autobc", None)
        raise  # Re-raise untuk menyelesaikan pembatalan
    except Exception as e:
        print(f"[CRITICAL] Error fatal pada autobc_task: {e}")
        print(f"[DEBUG] Stack trace: {traceback.format_exc()}")
        # Cleanup pada kasus error fatal
        async with AG_lock:
            if client.me.id in AG:
                AG.remove(client.me.id)
        await set_vars(client.me.id, "AUTO_GCAST_ACTIVE", False)
        
        # Hapus referensi task
        async with task_lock:
            if client.me.id in active_tasks and "autobc" in active_tasks[client.me.id]:
                active_tasks[client.me.id].pop("autobc", None)
        
        # Notifikasi user tentang error fatal
        try:
            ggl = await EMO.GAGAL(client)
            await client.send_message(
                client.me.id,
                f"{ggl}auto_gcast: Terjadi kesalahan fatal, auto_gcast dihentikan: {str(e)}"
            )
        except Exception:
            pass

# Fungsi untuk menjalankan limit check sebagai task terpisah
async def limit_check_task(client):
    task_id = None
    try:
        task_id = f"limit_{client.me.id}_{int(time_module.time())}"
        print(f"[INFO] Memulai limit_check_task untuk user {client.me.id} (ID: {task_id})")
        
        while True:
            # Verifikasi status berdasarkan database dan list
            async with LT_lock:
                limit_active = await get_vars(client.me.id, "AUTO_LIMIT_CHECK_ACTIVE")
                if not limit_active or client.me.id not in LT:
                    print(f"[INFO] Limit check dinonaktifkan untuk user {client.me.id}")
                    if client.me.id in LT:
                        LT.remove(client.me.id)
                    await set_vars(client.me.id, "AUTO_LIMIT_CHECK_ACTIVE", False)
                    
                    # Hapus referensi task
                    async with task_lock:
                        if client.me.id in active_tasks and "limit_check" in active_tasks[client.me.id]:
                            active_tasks[client.me.id].pop("limit_check", None)
                    
                    break
                
            try:
                for x in range(2):
                    cmd_message = await client.send_message(client.me.id, ".limit")
                    await limit_cmd(client, cmd_message)
                    await asyncio.sleep(5)
                
                # Tunggu 20 menit sebelum cek limit berikutnya
                print(f"[INFO] Menunggu 20 menit untuk cek limit berikutnya")
                
                # Split sleep untuk responsif terhadap pembatalan
                for _ in range(240):  # 20 menit = 1200 detik, 1200/5 = 240 iterasi
                    # Cek status sebelum sleep berikutnya
                    async with LT_lock:
                        limit_active = await get_vars(client.me.id, "AUTO_LIMIT_CHECK_ACTIVE")
                        if not limit_active or client.me.id not in LT:
                            print(f"[INFO] Limit check dinonaktifkan selama menunggu")
                            break
                    await asyncio.sleep(5)
            except asyncio.CancelledError:
                print(f"[INFO] Task limit_check untuk user {client.me.id} dibatalkan (ID: {task_id})")
                raise  # Re-raise untuk menyelesaikan pembatalan
            except Exception as e:
                print(f"[ERROR] Error dalam siklus limit_check untuk user {client.me.id}: {e}")
                print(f"[DEBUG] Stack trace: {traceback.format_exc()}")
                # Tunggu sebentar sebelum mencoba lagi
                await asyncio.sleep(300)
    except asyncio.CancelledError:
        print(f"[INFO] Task limit_check untuk user {client.me.id} dibatalkan (ID: {task_id})")
        
        # Hapus referensi task
        async with task_lock:
            if client.me.id in active_tasks and "limit_check" in active_tasks[client.me.id]:
                active_tasks[client.me.id].pop("limit_check", None)
        
        raise  # Re-raise untuk menyelesaikan pembatalan
    except Exception as e:
        print(f"[CRITICAL] Error fatal pada limit_check_task: {e}")
        print(f"[DEBUG] Stack trace: {traceback.format_exc()}")
        
        async with LT_lock:
            if client.me.id in LT:
                LT.remove(client.me.id)
        await set_vars(client.me.id, "AUTO_LIMIT_CHECK_ACTIVE", False)
        
        # Hapus referensi task
        async with task_lock:
            if client.me.id in active_tasks and "limit_check" in active_tasks[client.me.id]:
                active_tasks[client.me.id].pop("limit_check", None)
            
        # Notifikasi user tentang error fatal
        try:
            ggl = await EMO.GAGAL(client)
            await client.send_message(
                client.me.id,
                f"{ggl}Auto limit check mengalami kesalahan fatal dan dihentikan. Error: {str(e)}"
            )
        except Exception:
            pass

async def limit_cmd(client, message):
    ggl = await EMO.GAGAL(client)
    sks = await EMO.BERHASIL(client)
    prs = await EMO.PROSES(client)
    pong = await EMO.PING(client)
    tion = await EMO.MENTION(client)
    yubot = await EMO.UBOT(client)
    
    try:
        await client.unblock_user("SpamBot")
        bot_info = await client.resolve_peer("SpamBot")
        msg = await message.reply(f"{prs}processing . . .")
        response = await client.invoke(
            StartBot(
                bot=bot_info,
                peer=bot_info,
                random_id=client.rnd_id(),
                start_param="start",
            )
        )
        await sleep(1)
        await msg.delete()
        status = await client.get_messages("SpamBot", response.updates[1].message.id + 1) 
        if status and hasattr(status, "text"):
            pjg = len(status.text)
            print(f"[DEBUG] Status text length: {pjg}")
            if pjg <= 100:
                if client.me.is_premium:
                    text = f"""
<blockquote>{pong} sᴛᴀᴛᴜs ᴀᴋᴜɴ ᴘʀᴇᴍɪᴜᴍ : ᴛʀᴜᴇ
{tion} ʟɪᴍɪᴛ ᴄʜᴇᴄᴋ : ᴀᴋᴜɴ ᴀɴᴅᴀ ᴛɪᴅᴀᴋ ᴅɪʙᴀᴛᴀsɪ
{yubot} ᴜʙᴏᴛ : {bot.me.mention}</blockquote>
"""
                else:
                    text = f"""
<blockquote>sᴛᴀᴛᴜs ᴀᴋᴜɴ : ʙᴇʟɪ ᴘʀᴇᴍ ᴅᴜʟᴜ ʏᴀ
ʟɪᴍɪᴛ ᴄʜᴇᴄᴋ : ᴀᴋᴜɴ ᴀɴᴅᴀ ᴛɪᴅᴀᴋ ᴅɪʙᴀᴛᴀsɪ
ᴜʙᴏᴛ : {bot.me.mention}</blockquote>
"""
                await client.send_message(message.chat.id, text)
                return await client.invoke(DeleteHistory(peer=bot_info, max_id=0, revoke=True))
            else:
                if client.me.is_premium:
                    text = f"""
<blockquote>{pong} sᴛᴀᴛᴜs ᴀᴋᴜɴ ᴘʀᴇᴍɪᴜᴍ : ᴛʀᴜᴇ
{tion} ʟɪᴍɪᴛ ᴄʜᴇᴄᴋ : ᴀᴋᴜɴ ᴀɴᴅᴀ ʙᴇʀᴍᴀsᴀʟᴀʜ
{yubot} ᴜʙᴏᴛ : {bot.me.mention}</blockquote>
"""
                else:
                    text = f"""
<blockquote>sᴛᴀᴛᴜs ᴀᴋᴜɴ : ʙᴇʟɪ ᴘʀᴇᴍ ᴅᴜʟᴜ ʏᴀ
ʟɪᴍɪᴛ ᴄʜᴇᴄᴋ : ᴀᴋᴜɴ ᴀɴᴅᴀ ʙᴇʀᴍᴀsᴀʟᴀʜ
ᴜʙᴏᴛ : {bot.me.mention}</blockquote>
"""
                await client.send_message(message.chat.id, text)
                return await client.invoke(DeleteHistory(peer=bot_info, max_id=0, revoke=True))
        else:
            print("[WARNING] Status tidak valid atau status.text tidak ada")
    except Exception as e:
        print(f"[ERROR] Error dalam limit_cmd: {e}")
        print(f"[DEBUG] Stack trace: {traceback.format_exc()}")
        try:
            await msg.edit(f"{ggl}Error dalam memeriksa limit: {str(e)}")
        except:
            pass

@PY.UBOT("bc|gikes")
@PY.TOP_CMD
async def gcast_handler(client, message):
    if client.me.id in gcast_progress:
        prs = await EMO.PROSES(client)
        return await message.reply(f"<blockquote><b>{prs} Proses broadcast sedang berjalan, mohon tunggu...</b></blockquote>")
        
    gcast_progress.append(client.me.id)
    
    robot = await EMO.ROBOT(client)
    terompet = await EMO.TEROMPET(client)
    centang = await EMO.CENTANG(client)
    pesan = await EMO.PESAN(client)
    jam = await EMO.JAM(client)
    silang = await EMO.SILANG(client)
    prs = await EMO.PROSES(client)
    
    _msg = f"<b>{prs} ᴍᴇᴍᴘʀᴏsᴇs...</b>"
    gcs = await message.reply(_msg)    
    command, text = extract_type_and_msg(message)

    if command not in ["group", "users", "all"] or not text:
        gcast_progress.remove(client.me.id)
        ggl = await EMO.GAGAL(client)
        return await gcs.edit(f"<blockquote><code>{message.text.split()[0]}</code> <b>[ᴛʏᴘᴇ] [ᴛᴇxᴛ/ʀᴇᴘʟʏ]</b> {ggl}</blockquote>")
    
    chats = await get_data_id(client, command)
    blacklist = await get_list_from_vars(client.me.id, "BL_ID")

    done = 0
    failed = 0
    try:
        for chat_id in chats:
            if client.me.id not in gcast_progress:
                sks = await EMO.BERHASIL(client)
                await gcs.edit(f"<blockquote><b>ᴘʀᴏsᴇs ɢᴄᴀsᴛ ʙᴇʀʜᴀsɪʟ ᴅɪ ʙᴀᴛᴀʟᴋᴀɴ !</b> {sks}</blockquote>")
                return
            if chat_id in blacklist or chat_id in BLACKLIST_CHAT:
                continue

            try:
                if message.reply_to_message:
                    # Copy pesan dengan semua atribut termasuk emoji premium
                    await message.reply_to_message.copy(chat_id)
                else:
                    await client.send_message(chat_id, text)
                done += 1
                # Tambahkan sleep kecil untuk rate limiting
                await asyncio.sleep(0.5)
            except FloodWait as e:
                await asyncio.sleep(e.value)
                try:
                    if message.reply_to_message:
                        await message.reply_to_message.copy(chat_id)
                    else:
                        await client.send_message(chat_id, text)
                    done += 1
                except (Exception, ChannelPrivate) as e:
                    print(f"[ERROR] Gagal kirim ke {chat_id} setelah FloodWait: {e}")
                    failed += 1
            except (Exception, ChannelPrivate) as e:
                print(f"[ERROR] Gagal kirim ke {chat_id}: {e}")
                failed += 1
    finally:
        # Pastikan selalu dihapus dari list progress
        if client.me.id in gcast_progress:
            gcast_progress.remove(client.me.id)

    await gcs.delete()
    _gcs = f"""
<blockquote>{robot} <b>Youre Broadcast Result</b>{terompet}
  {centang} <b>Success: {done}</b>
  {silang} <b>Failed: {failed}</b>
  {robot} <b>Task ID: {message.id}</b>
  {pesan} <b>Type: {command}</b>
  {jam} <b>Blacklist: {len(blacklist)}</b>
<b>My Bot: @{bot.me.username}</b></blockquote>
"""
    return await message.reply(_gcs)


@PY.UBOT("bcfd|cfd")
@PY.TOP_CMD
async def forward_broadcast(client, message):
    if client.me.id in gcast_progress:
        prs = await EMO.PROSES(client)
        return await message.reply(f"<blockquote><b>{prs} Proses broadcast sedang berjalan, mohon tunggu...</b></blockquote>")
        
    gcast_progress.append(client.me.id)
    
    robot = await EMO.ROBOT(client)
    terompet = await EMO.TEROMPET(client)
    centang = await EMO.CENTANG(client)
    pesan = await EMO.PESAN(client)
    jam = await EMO.JAM(client)
    silang = await EMO.SILANG(client)
    prs = await EMO.PROSES(client)
    ggl = await EMO.GAGAL(client)
    
    _msg = f"{prs} proceꜱꜱing..."
    gcs = await message.reply(_msg)

    command, text = extract_type_and_msg(message)
    
    if command not in ["group", "users", "all"] or not text:
        gcast_progress.remove(client.me.id)
        return await gcs.edit(f"{ggl} {message.text.split()[0]} type [reply]")

    if not message.reply_to_message:
        gcast_progress.remove(client.me.id)
        return await gcs.edit(f"{ggl} {message.text.split()[0]} type [reply]")

    chats = await get_data_id(client, command)
    blacklist = await get_list_from_vars(client.me.id, "BL_ID")

    done = 0
    failed = 0
    try:
        for chat_id in chats:
            if client.me.id not in gcast_progress:
                brhsl = await EMO.BERHASIL(client)
                await gcs.edit(f"<blockquote><b>ᴘʀᴏsᴇs broadcast forward ʙᴇʀʜᴀsɪʟ ᴅɪ ʙᴀᴛᴀʟᴋᴀɴ !</b> {brhsl}</blockquote>")
                return
                
            if chat_id in blacklist or chat_id in BLACKLIST_CHAT:
                continue

            try:
                if message.reply_to_message:
                    await message.reply_to_message.forward(chat_id)
                else:
                    await text.forward(chat_id)
                done += 1
                # Tambahkan sleep kecil untuk rate limiting
                await asyncio.sleep(0.5)
            except FloodWait as e:
                await asyncio.sleep(e.value)
                try:
                    if message.reply_to_message:
                        await message.reply_to_message.forward(chat_id)
                    else:
                        await text.forward(chat_id)
                    done += 1
                except Exception as e:
                    print(f"[ERROR] Gagal forward ke {chat_id} setelah FloodWait: {e}")
                    failed += 1
            except Exception as e:
                print(f"[ERROR] Gagal forward ke {chat_id}: {e}")
                failed += 1
                pass
    finally:
        # Pastikan selalu dihapus dari list progress
        if client.me.id in gcast_progress:
            gcast_progress.remove(client.me.id)

    await gcs.delete()
    _gcs = f"""
<blockquote>{robot} <b>Youre Broadcast Result</b>{terompet}
  {centang} <b>Success: {done}</b>
  {silang} <b>Failed: {failed}</b>
  {robot} <b>Task ID: {message.id}</b>
  {pesan} <b>Type: {command}</b>
  {jam} <b>Blacklist: {len(blacklist)}</b>
<b>My Bot: @{bot.me.username}</b></blockquote>
"""
    return await message.reply(_gcs)

@PY.UBOT("stopg")
@PY.TOP_CMD
async def stopg_handler(client, message):
    sks = await EMO.BERHASIL(client)
    ggl = await EMO.GAGAL(client)
    if client.me.id in gcast_progress:
        gcast_progress.remove(client.me.id)
        return await message.reply(f"<blockquote><b>ɢᴄᴀsᴛ ʙᴇʀʜᴀsɪʟ ᴅɪ ᴄᴀɴᴄᴇʟ</b> {sks}</blockquote>")
    else:
        return await message.reply(f"<blockquote><b>{ggl}ᴛɪᴅᴀᴋ ᴀᴅᴀ ɢᴄᴀsᴛ !!!</b></blockquote>")

@PY.BOT("bcast")
@PY.ADMIN
async def _(client, message):
    msg = await message.reply("<blockquote><b>okee proses Boy...</blockquote></b>\n\n<blockquote><b>mohon bersabar untuk menunggu proses broadcast sampai selesai</blockquote></b>", quote=True)

    send = get_message(message)
    if not send:
        return await msg.edit("mohon balaꜱ atau ketik ꜱeꜱuatu...")
        
    susers = await get_list_from_vars(client.me.id, "SAVED_USERS")
    done = 0
    for chat_id in susers:
        try:
            if message.reply_to_message:
                await send.forward(chat_id)
            else:
                await client.send_message(chat_id, send)
            done += 1
            # Tambahkan sleep kecil untuk rate limiting
            await asyncio.sleep(0.5)
        except FloodWait as e:
            await asyncio.sleep(e.value)
            try:
                if message.reply_to_message:
                    await send.forward(chat_id)
                else:
                    await client.send_message(chat_id, send)
                done += 1
            except Exception:
                pass
        except Exception:
            pass

    return await msg.edit(f"<blockquote><b>Pesan broadcast berhasil terkirim ke {done} user</blockquote></b>\n\n<blockquote><b><u>ᴍʏ ᴜsᴇʀʙᴏᴛ: <a href=https://t.me/KingzVvip_Bot?start=_tgr_wCrTHGtkNWZl>ᴋɪɴɢᴢ ᴠᴠɪᴘ</a></u></b></blockquote>")


@PY.UBOT("addbl")
@PY.TOP_CMD
async def _(client, message):
    prs = await EMO.PROSES(client)
    grp = await EMO.BL_GROUP(client)
    ktrn = await EMO.BL_KETERANGAN(client)
    _msg = f"{prs}proceꜱꜱing..."

    msg = await message.reply(_msg)
    try:
        chat_id = message.chat.id
        blacklist = await get_list_from_vars(client.me.id, "BL_ID")

        if chat_id in blacklist:
            txt = f"""
<blockquote><b>{grp} ɢʀᴏᴜᴘ: {message.chat.title}</blockquote></b>
<blockquote><b>{ktrn} ᴋᴇᴛᴇʀᴀɴɢᴀɴ: sᴜᴅᴀʜ ᴀᴅᴀ ᴅᴀʟᴀᴍ ʟɪsᴛ ʙʟᴀᴄᴋʟɪsᴛ</b>

<b><emoji id=5258093637450866522>🤖</emoji>sɪʟᴀʜᴋᴀɴ ᴋᴇᴛɪᴋ .listbl ᴜɴᴛᴜᴋ ᴍᴇʟɪʜᴀᴛ ʀᴏᴏᴍ ʏᴀɴɢ sᴜᴅᴀʜ ᴅɪ ʙʟᴀᴄᴋʟɪsᴛ</b></blockquote>

<blockquote><b><u>ᴍʏ ᴜsᴇʀʙᴏᴛ: <a href=https://t.me/KingzVvip_Bot?start=_tgr_wCrTHGtkNWZl>ᴋɪɴɢᴢ ᴠᴠɪᴘ</a></u></b></blockquote>
"""
        else:
            await add_to_vars(client.me.id, "BL_ID", chat_id)
            txt = f"""
<blockquote><b>{grp} ɢʀᴏᴜᴘ: {message.chat.title}</blockquote></b>\n<blockquote><b>{ktrn} ᴋᴇᴛ: ʙᴇʀʜᴀsɪʟ ᴅɪ ᴛᴀᴍʙᴀʜᴋᴀɴ ᴋᴇ ᴅᴀʟᴀᴍ ʟɪsᴛ ʙʟᴀᴄᴋʟɪsᴛ ᴋɪɴɢᴢ</blockquote></b>

<blockquote><b><u>ᴍʏ ᴜsᴇʀʙᴏᴛ: <a href=https://t.me/KingzVvip_Bot?start=_tgr_wCrTHGtkNWZl>ᴋɪɴɢᴢ ᴠᴠɪᴘ</a></u></b></blockquote>
"""

        return await msg.edit(txt)
    except Exception as error:
        return await msg.edit(str(error))


@PY.UBOT("unbl")
@PY.TOP_CMD
async def _(client, message):
    prs = await EMO.PROSES(client)
    grp = await EMO.BL_GROUP(client)
    ktrn = await EMO.BL_KETERANGAN(client)
    _msg = f"{prs}proceꜱꜱing..."

    msg = await message.reply(_msg)
    try:
        chat_id = get_arg(message) or message.chat.id
        blacklist = await get_list_from_vars(client.me.id, "BL_ID")

        if chat_id not in blacklist:
            response = f"""
<blockquote><b>{grp} ɢʀᴏᴜᴘ: {message.chat.title}</blockquote></b>
<blockquote><b>{ktrn} ᴋᴇᴛ: ᴛɪᴅᴀᴋ ᴀᴅᴀ ᴅᴀʟᴀᴍ ʟɪsᴛ ʙʟᴀᴄᴋʟɪsᴛ</b></blockquote>

<blockquote><b><u>ᴍʏ ᴜsᴇʀʙᴏᴛ: <a href=https://t.me/KingzVvip_Bot?start=_tgr_wCrTHGtkNWZl>ᴋɪɴɢᴢ ᴠᴠɪᴘ</a></u></b></blockquote>
"""
        else:
            await remove_from_vars(client.me.id, "BL_ID", chat_id)
            response = f"""
<blockquote><b>{grp} ɢʀᴏᴜᴘ: {message.chat.title}</blockquote ></b>
<blockquote><b>{ktrn} ᴋᴇᴛ: ʙᴇʀʜᴀsɪʟ ᴅɪ ʜᴀᴘᴜs ᴋᴇ ᴅᴀʟᴀᴍ ʟɪsᴛ ʙʟᴀᴄᴋʟɪsᴛ</blockquote></b>

<blockquote><b><u>ᴍʏ ᴜsᴇʀʙᴏᴛ: <a href=https://t.me/KingzVvip_Bot?start=_tgr_wCrTHGtkNWZl>ᴋɪɴɢᴢ ᴠᴠɪᴘ</a></u></b></blockquote>
"""

        return await msg.edit(response)
    except Exception as error:
        return await msg.edit(str(error))


@PY.UBOT("listbl")
@PY.TOP_CMD
async def _(client, message):
    prs = await EMO.PROSES(client)
    brhsl = await EMO.BERHASIL(client)
    ktrng = await EMO.BL_KETERANGAN(client)
    _msg = f"{prs}proceꜱꜱing..."
    mzg = await message.reply(_msg)

    blacklist = await get_list_from_vars(client.me.id, "BL_ID")
    total_blacklist = len(blacklist)

    list = f"{brhsl} daftar blackliꜱt\n"

    for chat_id in blacklist:
        try:
            chat = await client.get_chat(chat_id)
            list += f" ├ {chat.title} | {chat.id}\n"
        except:
            list += f" ├ {chat_id}\n"

    list += f"{ktrng} total blackliꜱt {total_blacklist}"
    return await mzg.edit(list)


@PY.UBOT("rallbl")
@PY.TOP_CMD
async def _(client, message):
    prs = await EMO.PROSES(client)
    ggl = await EMO.GAGAL(client)
    brhsl = await EMO.BERHASIL(client)
    _msg = f"{prs}proceꜱꜱing..."

    msg = await message.reply(_msg)
    blacklists = await get_list_from_vars(client.me.id, "BL_ID")

    if not blacklists:
        return await msg.edit(f"{ggl}blackliꜱt broadcaꜱt anda koꜱong")

    for chat_id in blacklists:
        await remove_from_vars(client.me.id, "BL_ID", chat_id)

    await msg.edit(f"{brhsl}ꜱemua blackliꜱt broadcaꜱt berhaꜱil di hapuꜱ")


@PY.UBOT("send")
@PY.TOP_CMD
async def _(client, message):
    if message.reply_to_message:
        chat_id = (
            message.chat.id if len(message.command) < 2 else message.text.split()[1]
        )
        try:
            if client.me.id != bot.me.id:
                if message.reply_to_message.reply_markup:
                    x = await client.get_inline_bot_results(
                        bot.me.username, f"get_send {id(message)}"
                    )
                    return await client.send_inline_bot_result(
                        chat_id, x.query_id, x.results[0].id
                    )
        except Exception as error:
            return await message.reply(error)
        else:
            try:
                # Mendukung emoji premium dengan copy pesan asli
                return await message.reply_to_message.copy(chat_id)
            except Exception as t:
                return await message.reply(f"{t}")
    else:
        if len(message.command) < 3:
            return await message.reply("Ketik yang bener kntl")
        chat_id, chat_text = message.text.split(None, 2)[1:]
        try:
            if "_" in chat_id:
                msg_id, to_chat = chat_id.split("_")
                return await client.send_message(
                    to_chat, chat_text, reply_to_message_id=int(msg_id)
                )
            else:
                # Kirim pesan dengan dukungan emoji premium
                return await client.send_message(chat_id, chat_text)
        except Exception as t:
            return await message.reply(f"{t}")


@PY.INLINE("^get_send")
async def _(client, inline_query):
    _id = int(inline_query.query.split()[1])
    m = next((obj for obj in get_objects() if id(obj) == _id), None)
    if m:
        await client.answer_inline_query(
            inline_query.id,
            cache_time=0,
            results=[
                InlineQueryResultArticle(
                    title="get send!",
                    reply_markup=m.reply_to_message.reply_markup,
                    input_message_content=InputTextMessageContent(
                        m.reply_to_message.text
                    ),
                )
            ],
        )

# Fungsi untuk handling copy pesan dengan dukungan emoji premium
async def send_premium_message(client, chat_id, text, reply_to=None):
    """
    Kirim pesan dengan dukungan untuk emoji premium
    
    Args:
        client: Instance client Pyrogram
        chat_id: ID chat tujuan
        text: Teks pesan
        reply_to: ID pesan untuk dibalas (opsional)
    """
    try:
        # Pyrogram 3.0.2 menangani emoji premium secara otomatis saat copy
        return await client.send_message(
            chat_id=chat_id,
            text=text,
            reply_to_message_id=reply_to
        )
    except Exception as e:
        print(f"[ERROR] Error sending premium message: {e}")
        print(f"[DEBUG] Stack trace: {traceback.format_exc()}")
        # Fallback to regular message
        return await client.send_message(chat_id, text, reply_to_message_id=reply_to)


# Modified autobc command handler
@PY.UBOT("autobc")
@PY.TOP_CMD
async def _(client, message):
    try:
        prs = await EMO.PROSES(client)
        brhsl = await EMO.BERHASIL(client)
        bcs = await EMO.BROADCAST(client)
        mng = await EMO.MENUNGGU(client)
        ggl = await EMO.GAGAL(client)   
        
        msg = await message.reply(f"{prs}proceꜱꜱing...")
        type, value = extract_type_and_text(message)
        print(f"[INFO] Command autobc dijalankan oleh user {client.me.id} dengan type={type}, value={value}")
        
        auto_text_vars = await get_vars(client.me.id, "AUTO_TEXT")

        # Menambahkan command untuk melihat status broadcast terakhir
        if type == "status":
            try:
                # Get status from database flag instead of in-memory list
                autobc_active = await get_vars(client.me.id, "AUTO_GCAST_ACTIVE")
                is_active = autobc_active or (client.me.id in AG)  # Check both for reliability
                active_status = "aktif" if is_active else "tidak aktif"
                
                last_broadcast_info = await get_vars(client.me.id, "LAST_BROADCAST_INFO")
                
                if not last_broadcast_info:
                    return await msg.edit(f"{brhsl}Status auto broadcast:\n• Status: {active_status}\n• Belum ada broadcast yang dilakukan")
                
                if isinstance(last_broadcast_info, dict):
                    timestamp = last_broadcast_info.get("timestamp", 0)
                    delay = last_broadcast_info.get("delay", 0)
                    putaran = last_broadcast_info.get("putaran", 0)
                    
                    # Hitung waktu berlalu
                    current_time = time_module.time()
                    elapsed_seconds = current_time - timestamp
                    elapsed_minutes = elapsed_seconds / 60
                    
                    # Hitung waktu tersisa sampai broadcast berikutnya (jika aktif)
                    remaining_minutes = 0
                    if is_active:
                        delay_seconds = int(delay) * 60
                        if elapsed_seconds < delay_seconds:
                            remaining_minutes = (delay_seconds - elapsed_seconds) / 60
                    
                    # Format timestamp menjadi tanggal dan waktu yang mudah dibaca
                    broadcast_time = dt.datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")
                    
                    status_text = f"{brhsl}Status auto broadcast:\n"
                    status_text += f"• Status: {active_status}\n"
                    status_text += f"• Broadcast terakhir: {broadcast_time}\n"
                    status_text += f"• Putaran terakhir: {putaran}\n"
                    status_text += f"• Delay yang diatur: {delay} menit\n"
                    status_text += f"• Waktu berlalu: {elapsed_minutes:.2f} menit\n"
                    
                    if is_active and remaining_minutes > 0:
                        status_text += f"• Estimasi waktu tersisa: {remaining_minutes:.2f} menit"
                    
                    # Tambahkan informasi mode broadcast
                    forward_mode = await get_vars(client.me.id, "AUTOBC_FORWARD_MODE") or False
                    mode_text = "FORWARD" if forward_mode else "COPY (dengan dukungan emoji premium)"
                    status_text += f"\n• Mode broadcast: {mode_text}"
                    
                    # Tambahkan informasi tentang task aktif
                    async with task_lock:
                        has_active_task = (client.me.id in active_tasks and 
                                           "autobc" in active_tasks[client.me.id] and 
                                           not active_tasks[client.me.id]["autobc"].done())
                    
                    status_text += f"\n• Task aktif: {'Ya' if has_active_task else 'Tidak'}"
                    
                    return await msg.edit(status_text)
                else:
                    return await msg.edit(f"{brhsl}Status auto broadcast:\n• Status: {active_status}\n• Data broadcast tidak valid")
            except Exception as e:
                print(f"[ERROR] Error saat memeriksa status: {e}")
                print(f"[DEBUG] Stack trace: {traceback.format_exc()}")
                return await msg.edit(f"{ggl}Error checking status: {str(e)}")

        elif type == "on":
            try:
                if not auto_text_vars:
                    return await msg.edit(f"{ggl}harap ꜱetting text terlebih dahulu")

                # Check if the command includes the "forward" option
                forward_mode = False
                if value and value.lower() == "forward":
                    forward_mode = True
                    await set_vars(client.me.id, "AUTOBC_FORWARD_MODE", True)
                else:
                    await set_vars(client.me.id, "AUTOBC_FORWARD_MODE", False)

                # Check if already active
                autobc_active = await get_vars(client.me.id, "AUTO_GCAST_ACTIVE")
                
                # Periksa tidak hanya di AG list tetapi juga task yang sedang berjalan
                async with task_lock:
                    task_running = (client.me.id in active_tasks and 
                                    "autobc" in active_tasks[client.me.id] and 
                                    not active_tasks[client.me.id]["autobc"].done() and
                                    not active_tasks[client.me.id]["autobc"].cancelled())
                
                async with AG_lock:
                    is_already_active = autobc_active or client.me.id in AG or task_running
                
                if is_already_active:
                    # Jika task sedang berjalan tapi tidak konsisten dengan AG atau database, perbaiki
                    if task_running and (client.me.id not in AG or not autobc_active):
                        async with AG_lock:
                            if client.me.id not in AG:
                                AG.append(client.me.id)
                        await set_vars(client.me.id, "AUTO_GCAST_ACTIVE", True)
                        return await msg.edit(f"{brhsl}Auto broadcast sudah aktif (status disinkronkan)")
                    
                    return await msg.edit(f"{ggl}Auto broadcast sudah aktif")

                # Set flag di database bahwa autobc aktif
                await set_vars(client.me.id, "AUTO_GCAST_ACTIVE", True)
                
                # Simpan timestamp saat ini sebagai waktu broadcast terakhir
                current_time = time_module.time()
                delay = await get_vars(client.me.id, "DELAY_GCAST") or 1
                broadcast_info = {
                    "timestamp": current_time,
                    "delay": delay,
                    "putaran": 0
                }
                await set_vars(client.me.id, "LAST_BROADCAST_INFO", broadcast_info)
                
                mode_text = "FORWARD" if forward_mode else "COPY"
                
                # Add to active list before starting task
                async with AG_lock:
                    if client.me.id not in AG:
                        AG.append(client.me.id)
                print(f"[INFO] Mengaktifkan autobc untuk user {client.me.id} dengan mode {mode_text}")
                
                # Jalankan autobc task di background dengan proper error handling
                try:
                    # Cek dan batalkan task yang mungkin sudah ada tapi tidak terdeteksi
                    async with task_lock:
                        if client.me.id in active_tasks and "autobc" in active_tasks[client.me.id]:
                            old_task = active_tasks[client.me.id]["autobc"]
                            if not old_task.done() and not old_task.cancelled():
                                print(f"[WARNING] Task autobc untuk {client.me.id} sudah berjalan, membatalkan...")
                                old_task.cancel()
                        
                        # Buat task baru dengan ID unik
                        if client.me.id not in active_tasks:
                            active_tasks[client.me.id] = {}
                        
                        new_task = asyncio.create_task(autobc_task(client))
                        active_tasks[client.me.id]["autobc"] = new_task
                        print(f"[DEBUG] Task autobc baru dibuat dengan ID: {id(new_task)}")
                    
                    await msg.edit(f"{brhsl}auto gcaꜱt di aktifkan (Mode: {mode_text})")
                except Exception as e:
                    print(f"[ERROR] Error saat memulai autobc task: {e}")
                    print(f"[DEBUG] Stack trace: {traceback.format_exc()}")
                    # If task fails to start, remove from active list and reset flag
                    async with AG_lock:
                        if client.me.id in AG:
                            AG.remove(client.me.id)
                    await set_vars(client.me.id, "AUTO_GCAST_ACTIVE", False)
                    await msg.edit(f"{ggl}Error starting autobc: {str(e)}")
            except Exception as e:
                print(f"[ERROR] Error saat mengaktifkan autobc: {e}")
                print(f"[DEBUG] Stack trace: {traceback.format_exc()}")
                return await msg.edit(f"{ggl}Error activating autobc: {str(e)}")

        elif type == "off":
            try:
                # Check both database flag and in-memory list and task
                autobc_active = await get_vars(client.me.id, "AUTO_GCAST_ACTIVE")
                
                async with task_lock:
                    task_running = (client.me.id in active_tasks and 
                                    "autobc" in active_tasks[client.me.id] and 
                                    not active_tasks[client.me.id]["autobc"].done() and 
                                    not active_tasks[client.me.id]["autobc"].cancelled())
                
                async with AG_lock:
                    is_active = autobc_active or client.me.id in AG or task_running
                
                if is_active:
                    print(f"[INFO] Menonaktifkan autobc untuk user {client.me.id}")
                    # Remove from active list if present
                    async with AG_lock:
                        if client.me.id in AG:
                            AG.remove(client.me.id)
                    
                    # Batalkan task jika sedang berjalan
                    async with task_lock:
                        if (client.me.id in active_tasks and 
                            "autobc" in active_tasks[client.me.id] and 
                            not active_tasks[client.me.id]["autobc"].done()):
                            # Batalkan task dengan aman
                            try:
                                task = active_tasks[client.me.id]["autobc"]
                                if not task.cancelled():
                                    task.cancel()
                                    print(f"[INFO] Task autobc untuk {client.me.id} dibatalkan")
                            except Exception as e:
                                print(f"[WARNING] Error saat membatalkan task: {e}")
                            
                            # Hapus referensi task
                            active_tasks[client.me.id].pop("autobc", None)
                    
                    # Set flag di database bahwa autobc nonaktif
                    await set_vars(client.me.id, "AUTO_GCAST_ACTIVE", False)
                    return await msg.edit(f"{brhsl}auto gcast dinonaktifkan")
                else:
                    return await msg.edit(f"{ggl}Auto broadcast tidak aktif")
            except Exception as e:
                print(f"[ERROR] Error saat menonaktifkan autobc: {e}")
                print(f"[DEBUG] Stack trace: {traceback.format_exc()}")
                return await msg.edit(f"{ggl}Error deactivating autobc: {str(e)}")

        elif type == "timer":
            try:
                # Format yang diharapkan: "7:00-12:00" untuk mengatur timer dari jam 7 pagi hingga jam 12 siang
                if not value or "-" not in value:
                    return await msg.edit(
                        f"{ggl}{message.text.split()[0]} timer - [start_time-end_time] (format: HH:MM-HH:MM)"
                    )
                
                start_time, end_time = value.split("-")
                start_time = start_time.strip()
                end_time = end_time.strip()
                
                # Validasi format waktu dengan handling lebih baik
                try:
                    # Pastikan format HH:MM
                    if not (len(start_time.split(":")) == 2 and len(end_time.split(":")) == 2):
                        return await msg.edit(f"{ggl}Format waktu tidak valid. Gunakan format 24 jam (HH:MM).")
                    
                    start_hours, start_minutes = map(int, start_time.split(":"))
                    end_hours, end_minutes = map(int, end_time.split(":"))
                    
                    if not (0 <= start_hours < 24 and 0 <= start_minutes < 60 and 
                            0 <= end_hours < 24 and 0 <= end_minutes < 60):
                        return await msg.edit(f"{ggl}Format waktu tidak valid. Gunakan format 24 jam (HH:MM).")
                    
                    # Format the time with leading zeros
                    start_time = f"{start_hours:02d}:{start_minutes:02d}"
                    end_time = f"{end_hours:02d}:{end_minutes:02d}"
                    
                except ValueError:
                    return await msg.edit(f"{ggl}Format waktu tidak valid. Gunakan format 24 jam (HH:MM).")
                
                # Simpan pengaturan timer
                timer_settings = {
                    "enabled": True,
                    "start_time": start_time,
                    "end_time": end_time
                }
                await set_vars(client.me.id, "AUTOBC_TIMER", timer_settings)
                
                # Mulai timer checker task jika belum berjalan
                async with timer_lock:
                    if client.me.id not in timer_checker_users:
                        timer_checker_users.append(client.me.id)
                        
                        # Buat task baru dengan penanganan yang lebih baik
                        async with task_lock:
                            # Cek dan batalkan task yang mungkin sudah berjalan
                            if client.me.id in active_tasks and "timer" in active_tasks[client.me.id]:
                                old_task = active_tasks[client.me.id]["timer"]
                                if not old_task.done() and not old_task.cancelled():
                                    print(f"[WARNING] Task timer untuk {client.me.id} sudah berjalan, membatalkan...")
                                    old_task.cancel()
                            
                            # Pastikan ada entry untuk user ini
                            if client.me.id not in active_tasks:
                                active_tasks[client.me.id] = {}
                            
                            # Buat task baru
                            new_task = asyncio.create_task(timer_checker_task(client))
                            active_tasks[client.me.id]["timer"] = new_task
                            print(f"[INFO] Memulai timer checker untuk user {client.me.id}")
                
                return await msg.edit(
                    f"{brhsl}Timer auto broadcast berhasil diatur dari {start_time} sampai {end_time}"
                )
            except Exception as e:
                print(f"[ERROR] Error saat mengatur timer: {e}")
                print(f"[DEBUG] Stack trace: {traceback.format_exc()}")
                return await msg.edit(f"{ggl}Error: {str(e)}")
                
        elif type == "timer_off":
            try:
                timer_settings = await get_vars(client.me.id, "AUTOBC_TIMER") or {}
                if timer_settings:
                    timer_settings["enabled"] = False
                    await set_vars(client.me.id, "AUTOBC_TIMER", timer_settings)
                    
                    # Batalkan task timer jika sedang berjalan
                    async with task_lock:
                        if (client.me.id in active_tasks and 
                            "timer" in active_tasks[client.me.id] and 
                            not active_tasks[client.me.id]["timer"].done()):
                            try:
                                task = active_tasks[client.me.id]["timer"]
                                if not task.cancelled():
                                    task.cancel()
                                    print(f"[INFO] Task timer untuk {client.me.id} dibatalkan")
                            except Exception as e:
                                print(f"[WARNING] Error saat membatalkan task timer: {e}")
                            
                            # Hapus referensi task
                            active_tasks[client.me.id].pop("timer", None)
                    
                    async with timer_lock:
                        if client.me.id in timer_checker_users:
                            timer_checker_users.remove(client.me.id)
                            print(f"[INFO] Menonaktifkan timer checker untuk user {client.me.id}")
                    
                    return await msg.edit(f"{brhsl}Timer auto broadcast dinonaktifkan")
                else:
                    return await msg.edit(f"{ggl}Timer belum diatur")
            except Exception as e:
                print(f"[ERROR] Error saat menonaktifkan timer: {e}")
                print(f"[DEBUG] Stack trace: {traceback.format_exc()}")
                return await msg.edit(f"{ggl}Error: {str(e)}")
                
        elif type == "timer_status":
            try:
                timer_settings = await get_vars(client.me.id, "AUTOBC_TIMER") or {}
                if not timer_settings:
                    return await msg.edit(f"{ggl}Timer belum diatur")
                    
                enabled = timer_settings.get("enabled", False)
                start_time = timer_settings.get("start_time", "")
                end_time = timer_settings.get("end_time", "")
                
                status = "Aktif" if enabled else "Nonaktif"
                
                # Ambil waktu saat ini untuk memberikan konteks
                now = dt.datetime.now().strftime("%H:%M")
                should_be_active = False
                if enabled and start_time and end_time:
                    should_be_active = is_time_between(start_time, end_time, now)
                
                active_status = "Sedang aktif (dalam rentang waktu)" if should_be_active else "Tidak aktif saat ini (di luar rentang waktu)"
                
                # Cek apakah task timer sedang berjalan
                async with task_lock:
                    task_running = (client.me.id in active_tasks and 
                                    "timer" in active_tasks[client.me.id] and 
                                    not active_tasks[client.me.id]["timer"].done() and
                                    not active_tasks[client.me.id]["timer"].cancelled())
                
                status_text = f"{brhsl}Status timer auto broadcast:\n"
                status_text += f"• Konfigurasi: {status}\n"
                status_text += f"• Waktu mulai: {start_time}\n"
                status_text += f"• Waktu selesai: {end_time}\n"
                status_text += f"• Waktu saat ini: {now}\n"
                status_text += f"• Task timer aktif: {'Ya' if task_running else 'Tidak'}\n"
                if enabled:
                    status_text += f"• Status saat ini: {active_status}"
                
                return await msg.edit(status_text)
            except Exception as e:
                print(f"[ERROR] Error saat memeriksa status timer: {e}")
                print(f"[DEBUG] Stack trace: {traceback.format_exc()}")
                return await msg.edit(f"{ggl}Error: {str(e)}")

        elif type == "text":
            try:
                # Store message if replying to a message (support premium emoji)
                if message.reply_to_message:
                    if await add_auto_message(client, message):
                        return await msg.edit(f"{brhsl}pesan berhasil disimpan (dengan dukungan emoji premium)")
                    else:
                        return await msg.edit(f"{ggl}Gagal menyimpan pesan")
                # Store text
                elif value:
                    if await add_auto_message(client, message, value):
                        return await msg.edit(f"{brhsl}teks berhasil disimpan")
                    else:
                        return await msg.edit(f"{ggl}Gagal menyimpan teks")
                else:
                    return await msg.edit(
                        f"{ggl}{message.text.split()[0]} text - [value] atau reply ke pesan"
                    )
            except Exception as e:
                print(f"[ERROR] Error saat menyimpan pesan/teks: {e}")
                print(f"[DEBUG] Stack trace: {traceback.format_exc()}")
                return await msg.edit(f"{ggl}Error: {str(e)}")

        elif type == "delay":
            try:
                if not value or not value.isdigit():
                    return await msg.edit(
                        f"{ggl}{message.text.split()[0]} delay - [value numerik]"
                    )
                
                delay_value = int(value)
                if delay_value < 1:
                    return await msg.edit(f"{ggl}Delay minimum adalah 1 menit")
                
                await set_vars(client.me.id, "DELAY_GCAST", delay_value)
                print(f"[INFO] Mengatur delay broadcast menjadi {delay_value} menit untuk user {client.me.id}")
                
                # Update juga di informasi broadcast terakhir jika ada
                last_broadcast_info = await get_vars(client.me.id, "LAST_BROADCAST_INFO")
                if last_broadcast_info and isinstance(last_broadcast_info, dict):
                    last_broadcast_info["delay"] = delay_value
                    await set_vars(client.me.id, "LAST_BROADCAST_INFO", last_broadcast_info)
                
                return await msg.edit(
                    f"{brhsl}delay berhasil disetting menjadi {delay_value} menit"
                )
            except Exception as e:
                print(f"[ERROR] Error saat mengatur delay: {e}")
                print(f"[DEBUG] Stack trace: {traceback.format_exc()}")
                return await msg.edit(f"{ggl}Error: {str(e)}")

        elif type == "remove":
            try:
                if not value:
                    return await msg.edit(
                        f"{ggl}{message.text.split()[0]} remove - [value]"
                    )
                if value == "all":
                    await set_vars(client.me.id, "AUTO_TEXT", [])
                    print(f"[INFO] Menghapus semua pesan autobc untuk user {client.me.id}")
                    return await msg.edit(f"{brhsl}semua pesan autobc berhasil dihapus")
                try:
                    value = int(value) - 1
                    if not auto_text_vars:
                        return await msg.edit(f"{ggl}tidak ada pesan tersimpan")
                        
                    if 0 <= value < len(auto_text_vars):
                        removed_msg = auto_text_vars.pop(value)
                        await set_vars(client.me.id, "AUTO_TEXT", auto_text_vars)
                        print(f"[INFO] Menghapus pesan ke-{value+1} dari autobc untuk user {client.me.id}")
                        
                        # Log yang lebih informatif tentang pesan yang dihapus
                        if isinstance(removed_msg, dict):
                            if removed_msg.get("type") == "message_ref":
                                msg_id = removed_msg.get("message_id")
                                print(f"[DEBUG] Pesan yang dihapus adalah referensi message_id={msg_id}")
                            elif removed_msg.get("type") == "text":
                                content = removed_msg.get("content", "")[:30]
                                print(f"[DEBUG] Pesan yang dihapus adalah teks: {content}...")
                        else:
                            print(f"[DEBUG] Pesan yang dihapus adalah teks legacy: {str(removed_msg)[:30]}...")
                            
                        return await msg.edit(
                            f"{brhsl}pesan ke {value+1} berhasil dihapus"
                        )
                    else:
                        return await msg.edit(f"{ggl}indeks tidak valid")
                except Exception as error:
                    print(f"[ERROR] Error saat menghapus pesan: {error}")
                    print(f"[DEBUG] Stack trace: {traceback.format_exc()}")
                    return await msg.edit(str(error))
            except Exception as e:
                print(f"[ERROR] Error pada command remove: {e}")
                print(f"[DEBUG] Stack trace: {traceback.format_exc()}")
                return await msg.edit(f"{ggl}Error: {str(e)}")

        elif type == "list":
            try:
                if not auto_text_vars:
                    return await msg.edit(f"{ggl}auto gcast pesan kosong")
                txt = "daftar auto gcast pesan\n\n"
                for num, x in enumerate(auto_text_vars, 1):
                    if isinstance(x, dict):
                        if x.get("type") == "message_ref":
                            txt += f"{num}> [Pesan dengan ID: {x.get('message_id')}]\n"
                        elif x.get("type") == "text":
                            txt += f"{num}> {x.get('content')}\n\n"
                    else:
                        # Legacy format
                        txt += f"{num}> {x}\n\n"
                txt += f"\nuntuk menghapus pesan:\n{message.text.split()[0]} remove [angka/all]"
                return await msg.edit(txt)
            except Exception as e:
                print(f"[ERROR] Error pada command list: {e}")
                print(f"[DEBUG] Stack trace: {traceback.format_exc()}")
                return await msg.edit(f"{ggl}Error: {str(e)}")

        elif type == "limit":
            try:
                if value == "off":
                    async with LT_lock:
                        is_active = client.me.id in LT
                        
                    # Cek juga task yang sedang berjalan
                    async with task_lock:
                        task_running = (client.me.id in active_tasks and 
                                        "limit_check" in active_tasks[client.me.id] and 
                                        not active_tasks[client.me.id]["limit_check"].done() and
                                        not active_tasks[client.me.id]["limit_check"].cancelled())
                    
                    if is_active or task_running:
                        async with LT_lock:
                            if client.me.id in LT:
                                LT.remove(client.me.id)
                        
                        # Batalkan task jika sedang berjalan
                        async with task_lock:
                            if (client.me.id in active_tasks and 
                                "limit_check" in active_tasks[client.me.id] and 
                                not active_tasks[client.me.id]["limit_check"].done()):
                                try:
                                    task = active_tasks[client.me.id]["limit_check"]
                                    if not task.cancelled():
                                        task.cancel()
                                        print(f"[INFO] Task limit_check untuk {client.me.id} dibatalkan")
                                except Exception as e:
                                    print(f"[WARNING] Error saat membatalkan task limit_check: {e}")
                                
                                # Hapus referensi task
                                active_tasks[client.me.id].pop("limit_check", None)
                        
                        # Set flag di database bahwa limit check nonaktif
                        await set_vars(client.me.id, "AUTO_LIMIT_CHECK_ACTIVE", False)
                        print(f"[INFO] Menonaktifkan auto limit check untuk user {client.me.id}")
                        return await msg.edit(f"{brhsl}auto cek limit dinonaktifkan")
                    else:
                        return await msg.edit(f"{ggl}Auto cek limit sudah nonaktif")

                elif value == "on":
                    async with LT_lock:
                        is_active = client.me.id in LT
                    
                    # Cek juga task yang sedang berjalan
                    async with task_lock:
                        task_running = (client.me.id in active_tasks and 
                                        "limit_check" in active_tasks[client.me.id] and 
                                        not active_tasks[client.me.id]["limit_check"].done() and
                                        not active_tasks[client.me.id]["limit_check"].cancelled())
                        
                    if not (is_active or task_running):
                        # Set flag di database bahwa limit check aktif
                        await set_vars(client.me.id, "AUTO_LIMIT_CHECK_ACTIVE", True)
                        
                        async with LT_lock:
                            if client.me.id not in LT:
                                LT.append(client.me.id)
                            
                        print(f"[INFO] Mengaktifkan auto limit check untuk user {client.me.id}")
                        
                        # Pastikan hanya ada satu task yang berjalan
                        async with task_lock:
                            # Cek dan batalkan task yang mungkin sudah berjalan
                            if (client.me.id in active_tasks and 
                                "limit_check" in active_tasks[client.me.id] and 
                                not active_tasks[client.me.id]["limit_check"].done() and
                                not active_tasks[client.me.id]["limit_check"].cancelled()):
                                print(f"[WARNING] Task limit_check untuk {client.me.id} sudah berjalan")
                            else:
                                # Pastikan ada entry untuk user ini
                                if client.me.id not in active_tasks:
                                    active_tasks[client.me.id] = {}
                                
                                # Buat task baru dengan ID unik
                                new_task = asyncio.create_task(limit_check_task(client))
                                active_tasks[client.me.id]["limit_check"] = new_task
                                print(f"[DEBUG] Task limit_check baru dibuat dengan ID: {id(new_task)}")
                        
                        await msg.edit(f"{brhsl}auto cek limit started")
                    else:
                        # Jika sudah aktif tetapi mungkin tidak konsisten, perbaiki statusnya
                        if task_running and client.me.id not in LT:
                            async with LT_lock:
                                LT.append(client.me.id)
                            await set_vars(client.me.id, "AUTO_LIMIT_CHECK_ACTIVE", True)
                            return await msg.edit(f"{brhsl}Auto cek limit sudah aktif (status disinkronkan)")
                        
                        return await msg.edit(f"{ggl}Auto cek limit sudah aktif")
                else:
                    return await msg.edit(f"{ggl}{message.text.split()[0]} limit - [on/off]")
            except Exception as e:
                print(f"[ERROR] Error pada command limit: {e}")
                print(f"[DEBUG] Stack trace: {traceback.format_exc()}")
                return await msg.edit(f"{ggl}Error: {str(e)}")

        elif type == "mode":
            try:
                if value and value.lower() == "forward":
                    await set_vars(client.me.id, "AUTOBC_FORWARD_MODE", True)
                    print(f"[INFO] Mengubah mode autobc ke FORWARD untuk user {client.me.id}")
                    return await msg.edit(f"{brhsl}Mode autobc diubah ke FORWARD")
                elif value and value.lower() == "copy":
                    await set_vars(client.me.id, "AUTOBC_FORWARD_MODE", False)
                    print(f"[INFO] Mengubah mode autobc ke COPY untuk user {client.me.id}")
                    return await msg.edit(f"{brhsl}Mode autobc diubah ke COPY (dengan dukungan emoji premium)")
                else:
                    current_mode = await get_vars(client.me.id, "AUTOBC_FORWARD_MODE") or False
                    mode_text = "FORWARD" if current_mode else "COPY"
                    return await msg.edit(f"{brhsl}Mode autobc saat ini: {mode_text}\n\nUntuk mengubah mode: {message.text.split()[0]} mode [forward/copy]")
            except Exception as e:
                print(f"[ERROR] Error pada command mode: {e}")
                print(f"[DEBUG] Stack trace: {traceback.format_exc()}")
                return await msg.edit(f"{ggl}Error: {str(e)}")

        # Tambahkan command untuk restart task jika terjadi masalah
        elif type == "restart":
            try:
                # Cek status auto broadcast
                autobc_active = await get_vars(client.me.id, "AUTO_GCAST_ACTIVE")
                
                if not autobc_active:
                    return await msg.edit(f"{ggl}Auto broadcast tidak aktif. Gunakan '{message.text.split()[0]} on' untuk mengaktifkan.")
                
                # Dapatkan status task saat ini
                async with task_lock:
                    task_exists = client.me.id in active_tasks and "autobc" in active_tasks[client.me.id]
                    task_running = task_exists and not active_tasks[client.me.id]["autobc"].done()
                
                # Jika task sedang berjalan, batalkan terlebih dahulu
                if task_running:
                    async with task_lock:
                        try:
                            active_tasks[client.me.id]["autobc"].cancel()
                            print(f"[INFO] Task autobc untuk {client.me.id} dibatalkan untuk restart")
                        except Exception as e:
                            print(f"[WARNING] Error saat membatalkan task: {e}")
                
                # Pastikan flag di database dan list konsisten
                async with AG_lock:
                    if client.me.id not in AG:
                        AG.append(client.me.id)
                await set_vars(client.me.id, "AUTO_GCAST_ACTIVE", True)
                
                # Buat task baru
                async with task_lock:
                    if client.me.id not in active_tasks:
                        active_tasks[client.me.id] = {}
                    
                    new_task = asyncio.create_task(autobc_task(client))
                    active_tasks[client.me.id]["autobc"] = new_task
                    print(f"[INFO] Task autobc untuk {client.me.id} di-restart")
                
                return await msg.edit(f"{brhsl}Auto broadcast berhasil di-restart")
            except Exception as e:
                print(f"[ERROR] Error saat me-restart autobc: {e}")
                print(f"[DEBUG] Stack trace: {traceback.format_exc()}")
                return await msg.edit(f"{ggl}Error: {str(e)}")
        else:
            usage = f"{ggl}Penggunaan:\n"
            usage += f"• {message.text.split()[0]} on - Aktifkan autobc (mode copy)\n"
            usage += f"• {message.text.split()[0]} on forward - Aktifkan autobc mode forward\n"
            usage += f"• {message.text.split()[0]} off - Nonaktifkan autobc\n"
            usage += f"• {message.text.split()[0]} status - Cek status dan info broadcast\n"
            usage += f"• {message.text.split()[0]} text [teks] - Tambahkan teks\n"
            usage += f"• {message.text.split()[0]} text - Reply pesan untuk menyimpan dengan emoji premium\n"
            usage += f"• {message.text.split()[0]} mode [forward/copy] - Ubah mode broadcast\n"
            usage += f"• {message.text.split()[0]} delay [menit] - Atur jeda\n"
            usage += f"• {message.text.split()[0]} remove [nomor/all] - Hapus pesan\n"
            usage += f"• {message.text.split()[0]} list - Lihat daftar pesan\n"
            usage += f"• {message.text.split()[0]} timer [start:HH:MM-end:HH:MM] - Atur jadwal aktif otomatis\n"
            usage += f"• {message.text.split()[0]} timer_off - Nonaktifkan timer\n"
            usage += f"• {message.text.split()[0]} timer_status - Cek status timer\n"
            usage += f"• {message.text.split()[0]} limit [on/off] - Aktifkan/nonaktifkan cek limit\n"
            usage += f"• {message.text.split()[0]} restart - Restart task autobc jika ada masalah\n"
            usage += f"• Cara Menjalankan Auto Broadcasting:<a href=https://t.me/InfoKingzUserbot/28>ᴄʟɪᴄᴋ ᴋɪɴɢᴢ</a>\n"
            usage += f"• Cara Menggunakan timer auto broadcasting:<a href=https://t.me/InfoKingzUserbot/41>ᴄʟɪᴄᴋ ᴋɪɴɢᴢ</a>"
            return await msg.edit(usage)
    except Exception as e:
        print(f"[CRITICAL] Error fatal pada command handler autobc: {e}")
        print(f"[DEBUG] Stack trace: {traceback.format_exc()}")
        try:
            ggl = await EMO.GAGAL(client)
            return await msg.edit(f"{ggl}Terjadi kesalahan fatal: {str(e)}")
        except:
            pass

# Fungsi helper untuk menambahkan teks autobc (legacy)
async def add_auto_text(client, text):
    """Legacy function for backward compatibility"""
    auto_text = await get_vars(client.me.id, "AUTO_TEXT") or []
    # Check if auto_text contains dictionaries already (new format)
    if auto_text and isinstance(auto_text[0], dict):
        auto_text.append({"type": "text", "content": text})
    else:
        # Old format - just append the text
        auto_text.append(text)
    await set_vars(client.me.id, "AUTO_TEXT", auto_text)

# Fungsi helper untuk menambahkan pesan autobc
async def add_auto_message(client, message, text=None):
    try:
        print(f"[INFO] Menambahkan pesan autobc untuk user {client.me.id}")
        auto_messages = await get_vars(client.me.id, "AUTO_TEXT") or []
        
        # If replying to a message, store message reference instead of text
        if message.reply_to_message and not text:
            # Store as a dictionary with message_id and chat_id
            msg_data = {
                "type": "message_ref",
                "chat_id": message.chat.id,
                "message_id": message.reply_to_message.id
            }
            auto_messages.append(msg_data)
            print(f"[DEBUG] Menyimpan referensi pesan: chat_id={message.chat.id}, message_id={message.reply_to_message.id}")
        else:
            # Text storage
            if text:
                msg_data = {
                    "type": "text",
                    "content": text
                }
                auto_messages.append(msg_data)
                print(f"[DEBUG] Menyimpan pesan teks: {text[:30]}...")
            
        await set_vars(client.me.id, "AUTO_TEXT", auto_messages)
        print(f"[INFO] Berhasil menyimpan pesan autobc, total pesan: {len(auto_messages)}")
        return True
    except Exception as e:
        print(f"[ERROR] Gagal menambahkan pesan autobc: {e}")
        print(f"[DEBUG] Stack trace: {traceback.format_exc()}")
        return False

@PY.BOT("bcubot")
@PY.ADMIN
async def broadcast_bot(client, message):
    msg = await message.reply("<b>sᴇᴅᴀɴɢ ᴅɪᴘʀᴏsᴇs ᴛᴜɴɢɢᴜ sᴇʙᴇɴᴛᴀʀ</b>", quote=True)
    done = 0
    if not message.reply_to_message:
        return await msg.edit("<b>ᴍᴏʜᴏɴ ʙᴀʟᴀs ᴘᴇsᴀɴ</b>")
    for x in ubot._ubot:
        try:
            await x.unblock_user(bot.me.username)
            await message.reply_to_message.forward(x.me.id)
            done += 1
            # Tambahkan sleep kecil untuk rate limiting
            await asyncio.sleep(0.5)
        except Exception as e:
            print(f"[ERROR] Gagal broadcast ke ubot {x.me.id}: {e}")
            pass
    return await msg.edit(f"✅ ʙᴇʀʜᴀsɪʟ ᴍᴇɴɢɪʀɪᴍ ᴘᴇsᴀɴ ᴋᴇ {done} ᴜʙᴏᴛ")
