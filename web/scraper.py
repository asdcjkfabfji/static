import asyncio
import aiohttp
from database import save_to_db

# --- SOZLAMALAR ---
API_URL = "https://student.bstu.uz/rest/v1/data/student-list"
TOKEN = "szfm4g6aDB0g6I-oh0iUamskyjs5Plfx" # O'zingizning tokengingizni shu yerga qo'ying

async def get_all_data_stream():
    """
    Barcha talabalarni API'dan to'liq yuklaydi va har bir sahifa 
    yuklanganda natijani yield orqali uzatadi.
    """
    headers = {
        "Authorization": f"Bearer {TOKEN}",
        "Accept": "application/json",
        "Content-Type": "application/json"
    }

    # 1. Ulanish boshlanganini darhol bildiramiz
    yield "Ulanish o'rnatilmoqda..."

    async with aiohttp.ClientSession() as session:
        try:
            # 2. Dastlabki so'rov: Jami sahifalar sonini aniqlash
            async with session.get(API_URL, params={"limit": 100}, headers=headers, timeout=30) as resp:
                if resp.status != 200:
                    yield f"Xato: API ulanishda muammo (Status: {resp.status})"
                    return
                
                init_json = await resp.json()
                pagination = init_json.get('data', {}).get('pagination', {})
                total_pages = pagination.get('pageCount', 1)
                total_items = pagination.get('totalCount', 0)
                
                yield f"Boshlandi: Jami {total_items} ta talaba ({total_pages} sahifa) aniqlandi."

            # 3. Sahifama-sahifa yuklash
            current_count = 0
            
            for page in range(1, total_pages + 1):
                params = {"page": page, "limit": 100}
                
                try:
                    async with session.get(API_URL, params=params, headers=headers, timeout=30) as response:
                        if response.status == 200:
                            data = await response.json()
                            items = data.get('data', {}).get('items', [])
                            
                            if items:
                                # Har bir sahifa ma'lumotlarini darhol bazaga saqlaymiz
                                save_to_db(items)
                                current_count += len(items)
                                
                                # Brauzerga nechanchi talaba yuklanayotganini uzatamiz
                                yield f"Yuklandi: {current_count} / {total_items}"
                            else:
                                yield f"Ogohlantirish: {page}-sahifa bo'sh keldi."
                        else:
                            yield f"Xato: {page}-sahifada uzilish (Status: {response.status})"
                
                except Exception as e:
                    yield f"Xato: {page}-sahifani yuklashda xatolik: {str(e)}"
                
                # Oqim qotib qolmasligi uchun juda qisqa tanaffus
                await asyncio.sleep(0.01)

            yield f"Tayyor: Jami {current_count} ta talaba bazaga muvaffaqiyatli saqlandi!"

        except Exception as e:
            yield f"Kutilmagan xatolik: {str(e)}"