"""
Scraper — tv.senado.cl + camara.cl
========================================
Senado : articles con clase 'col span_1_of_4 article'
         link dentro de <a> que envuelve <h2 class="title">
         luego entra a la página individual y extrae el .mp4 de janux

Cámara : página television.aspx con URLs rtmp en atributos HTML escapados (&#39;)
         descarga con rtmpdump (protocolo rtmp nativo)
         convierte a MP3 con ffmpeg directamente

Bucket destino: gs://komv1/
  audio/          → MP3
  transcripciones/→ TXT
"""

import hashlib
import html as html_lib
import logging
import os
import re
import subprocess
from dataclasses import dataclass, field
from datetime import datetime, timezone
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from google.cloud import firestore, speech, storage

# ── Configuración ──────────────────────────────────────────────────────────────
GCP_PROJECT         = "crack-map-317501"
BUCKET_NAME         = "komv1"
FIRESTORE_COLECCION = "sesiones"
MP3_BITRATE         = "128k"
CARPETA_AUDIO       = "audio"
CARPETA_TX          = "transcripciones"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
}


# ── Modelo ─────────────────────────────────────────────────────────────────────
@dataclass
class Sesion:
    url_video: str       # URL directa al mp4 (janux) o rtmp (camara)
    titulo: str
    fuente: str          # "senado" | "camara"
    fecha_str: str = ""  # "2026-04-14"
    id: str = ""

    def __post_init__(self):
        self.id = hashlib.sha256(self.url_video.encode()).hexdigest()[:16]
        if not self.fecha_str:
            self.fecha_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    @property
    def ruta_audio(self):
        return f"{CARPETA_AUDIO}/{self.fuente}/{self.fecha_str}/{self.id}.mp3"

    @property
    def ruta_txt(self):
        return f"{CARPETA_TX}/{self.fuente}/{self.fecha_str}/{self.id}.txt"

    @property
    def uri_audio(self):
        return f"gs://{BUCKET_NAME}/{self.ruta_audio}"

    @property
    def uri_txt(self):
        return f"gs://{BUCKET_NAME}/{self.ruta_txt}"


# ── Helpers ────────────────────────────────────────────────────────────────────
def get_html(url: str) -> str:
    for intento in range(3):
        try:
            r = requests.get(url, headers=HEADERS, timeout=25)
            r.raise_for_status()
            return r.text
        except Exception as e:
            if intento == 2:
                raise
            log.warning(f"Reintento {intento+1} para {url}: {e}")


def parsear_fecha(texto: str) -> str:
    """dd/mm/yyyy o 'dd de mes de yyyy' → 'yyyy-mm-dd'"""
    meses = {
        "enero": "01", "febrero": "02", "marzo": "03", "abril": "04",
        "mayo": "05", "junio": "06", "julio": "07", "agosto": "08",
        "septiembre": "09", "octubre": "10", "noviembre": "11", "diciembre": "12"
    }
    m = re.search(r"(\d{1,2})/(\d{1,2})/(\d{4})", texto)
    if m:
        return f"{m.group(3)}-{m.group(2).zfill(2)}-{m.group(1).zfill(2)}"
    m = re.search(r"(\d{1,2})\s+de\s+(\w+)\s+de\s+(\d{4})", texto, re.I)
    if m:
        mes = meses.get(m.group(2).lower(), "01")
        return f"{m.group(3)}-{mes}-{m.group(1).zfill(2)}"
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


# ══════════════════════════════════════════════════════════════════════════════
# SCRAPER SENADO
# ══════════════════════════════════════════════════════════════════════════════
BASE_SENADO  = "https://tv.senado.cl"
URL_LISTA    = "https://tv.senado.cl/tvsenado/site/tax/port/all/taxport_7___{n}.html"
RE_JANUX     = re.compile(r'https?://janux-\d+\.senado\.cl/compactos/[\d/_]+\.mp4', re.I)


def _extraer_mp4_senado(url_sesion: str) -> str | None:
    """
    Entra a la página individual de una sesión del Senado
    y extrae la URL directa al .mp4 de janux-N.senado.cl
    Busca en: href de <a class="downloadVideo">, atributos data-, y texto del HTML.
    """
    try:
        html = get_html(url_sesion)
        soup = BeautifulSoup(html, "html.parser")

        # 1. Link con clase downloadVideo (botón "Descargar Video | X MB")
        for a in soup.select("a.downloadVideo, a[class*='download']"):
            href = a.get("href", "")
            if RE_JANUX.search(href):
                return href

        # 2. Cualquier <a> cuyo href apunte a janux
        for a in soup.find_all("a", href=RE_JANUX):
            return a.get("href")

        # 3. Buscar el patrón en el HTML completo (puede estar en JS o data-)
        m = RE_JANUX.search(html)
        if m:
            return m.group(0)

    except Exception as e:
        log.error(f"Error extrayendo MP4 de {url_sesion}: {e}")

    return None


def scrape_senado(max_paginas: int = 5) -> list[Sesion]:
    """
    Recorre las páginas de listado del Senado.
    Selector real: article con clases 'col span_1_of_4 article'
    El link a la sesión está en: div.text > a[href] > h2.title
    """
    todas = []

    for n in range(1, max_paginas + 1):
        url_pagina = URL_LISTA.format(n=n)
        try:
            html = get_html(url_pagina)
            soup = BeautifulSoup(html, "html.parser")

            # Selector real: article con esas tres clases
            articulos = soup.find_all(
                "article",
                class_=lambda c: c and "span_1_of_4" in c and "article" in c
            )

            if not articulos:
                log.info(f"Senado página {n}: sin artículos, deteniendo paginación")
                break

            log.info(f"Senado página {n}: {len(articulos)} artículos encontrados")

            for art in articulos:
                # El link envuelve el h2
                a_tag = art.select_one("div.text a[href]")
                if not a_tag:
                    continue

                href   = a_tag.get("href", "")
                titulo = a_tag.get_text(strip=True)

                # Fallback: buscar h2 dentro del link
                h2 = a_tag.find("h2")
                if h2:
                    titulo = h2.get_text(strip=True)

                url_sesion = urljoin(BASE_SENADO, href)

                # Fecha desde span.date
                fecha_tag = art.select_one("span.date")
                fecha = parsear_fecha(fecha_tag.get_text(strip=True)) if fecha_tag else ""

                if not titulo or not url_sesion:
                    continue

                # Entrar a la página individual para obtener el .mp4
                url_mp4 = _extraer_mp4_senado(url_sesion)
                if not url_mp4:
                    log.warning(f"Sin MP4 en: {url_sesion}")
                    continue

                todas.append(Sesion(
                    url_video = url_mp4,
                    titulo    = titulo,
                    fuente    = "senado",
                    fecha_str = fecha,
                ))

        except Exception as e:
            log.error(f"Error en página Senado {n}: {e}")
            break

    log.info(f"Senado total: {len(todas)} sesiones con video")
    return todas


# ══════════════════════════════════════════════════════════════════════════════
# SCRAPER CÁMARA
# ══════════════════════════════════════════════════════════════════════════════
URL_CAMARA = "https://camara.cl/prensa/television.aspx"

# El onclick está escapado como HTML entities: &#39; en vez de '
# Después de unescape queda: reproducirVideoLocal('rtmp://...', $(this), 'Título')
RE_CAMARA = re.compile(
    r"reproducirVideoLocal\(\s*'(rtmp://[^']+)'\s*,.*?'([^']+)'\s*\)",
    re.S
)


def scrape_camara() -> list[Sesion]:
    """
    Lee camara.cl/prensa/television.aspx.
    Los onclick están HTML-escapados (&#39;), hay que hacer unescape primero.
    """
    sesiones = []
    try:
        html_crudo = get_html(URL_CAMARA)

        # Desescapar HTML entities ANTES de parsear con BeautifulSoup
        # (BeautifulSoup a veces no expone el onclick escapado correctamente)
        html_limpio = html_lib.unescape(html_crudo)
        soup = BeautifulSoup(html_limpio, "html.parser")

        for tag in soup.find_all(onclick=True):
            onclick = tag.get("onclick", "")
            m = RE_CAMARA.search(onclick)
            if not m:
                continue

            url_rtmp = m.group(1).strip()
            titulo   = m.group(2).strip()

            # Buscar fecha en el contenedor padre
            fecha_txt = ""
            for padre in [tag.find_parent("article"),
                          tag.find_parent("div"),
                          tag.find_parent("li")]:
                if padre:
                    # Fecha en span con color azul corporativo
                    span = padre.find("span", style=lambda v: v and (
                        "#0066cc" in v or "#006" in v or "color" in v
                    ))
                    if span:
                        fecha_txt = span.get_text(strip=True)
                        break
                    # Alternativa: cualquier texto con formato dd/mm/yyyy
                    m_fecha = re.search(r"\d{1,2}/\d{1,2}/\d{4}", padre.get_text())
                    if m_fecha:
                        fecha_txt = m_fecha.group(0)
                        break

            fecha = parsear_fecha(fecha_txt) if fecha_txt else datetime.now(timezone.utc).strftime("%Y-%m-%d")

            sesiones.append(Sesion(
                url_video = url_rtmp,
                titulo    = titulo,
                fuente    = "camara",
                fecha_str = fecha,
            ))

    except Exception as e:
        log.error(f"Error scrapeando Cámara: {e}")

    # Eliminar duplicados por URL
    vistos, unicos = set(), []
    for s in sesiones:
        if s.url_video not in vistos:
            vistos.add(s.url_video)
            unicos.append(s)

    log.info(f"Cámara total: {len(unicos)} sesiones con video")
    return unicos


# ══════════════════════════════════════════════════════════════════════════════
# DESCARGA → MP3
# ══════════════════════════════════════════════════════════════════════════════
def _tiene_rtmpdump() -> bool:
    try:
        subprocess.run(["rtmpdump", "--help"],
                       capture_output=True, timeout=5)
        return True
    except FileNotFoundError:
        return False


def _descargar_rtmp_a_mp3(url_rtmp: str, ruta_mp3: str) -> bool:
    """
    Descarga un stream rtmp con rtmpdump y lo convierte a MP3 con ffmpeg
    en un solo pipeline, sin guardar el video completo en disco.

    rtmpdump -r URL -o - | ffmpeg -i pipe:0 -vn -ab 128k -f mp3 salida.mp3
    """
    log.info(f"Descargando RTMP → MP3: {url_rtmp[:70]}")

    rtmpdump_cmd = [
        "rtmpdump",
        "-r", url_rtmp,
        "-o", "-",           # output a stdout
        "--quiet",
    ]
    ffmpeg_cmd = [
        "ffmpeg",
        "-i", "pipe:0",      # input desde stdin
        "-vn",               # ignorar video
        "-acodec", "libmp3lame",
        "-ab", MP3_BITRATE,
        "-f", "mp3",
        "-y",                # sobreescribir si existe
        ruta_mp3,
    ]

    try:
        # Pipe directo entre rtmpdump y ffmpeg
        proc_rtmp  = subprocess.Popen(rtmpdump_cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
        proc_ffmpeg = subprocess.Popen(ffmpeg_cmd,  stdin=proc_rtmp.stdout,
                                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        proc_rtmp.stdout.close()
        proc_ffmpeg.wait(timeout=3600)
        proc_rtmp.wait(timeout=10)

        if proc_ffmpeg.returncode == 0 and os.path.exists(ruta_mp3):
            tam_mb = os.path.getsize(ruta_mp3) / 1_048_576
            log.info(f"MP3 listo: {ruta_mp3} ({tam_mb:.1f} MB)")
            return True
        else:
            log.error(f"ffmpeg terminó con código {proc_ffmpeg.returncode}")
            return False

    except subprocess.TimeoutExpired:
        log.error("Timeout en descarga RTMP")
        return False
    except Exception as e:
        log.error(f"Error en pipeline rtmp→mp3: {e}")
        return False


def descargar_mp3(sesion: Sesion, tmp: str = "/tmp") -> str | None:
    """
    Senado  → yt-dlp (URL mp4 directa de janux, funciona perfecto)
    Cámara  → rtmpdump + ffmpeg en pipeline (URL rtmp)
    """
    nombre   = f"{sesion.fuente}_{sesion.id}"
    ruta_mp3 = f"{tmp}/{nombre}.mp3"

    if sesion.fuente == "senado":
        # ── Senado: descarga directa con yt-dlp ──────────────────────────────
        plantilla = f"{tmp}/{nombre}.%(ext)s"
        cmd = [
            "yt-dlp",
            "--extract-audio",
            "--audio-format",  "mp3",
            "--audio-quality", MP3_BITRATE,
            "--output",        plantilla,
            "--no-playlist",
            "--quiet",
            "--no-warnings",
            sesion.url_video,
        ]
        log.info(f"Descargando (yt-dlp): {sesion.url_video[:80]}")
        res = subprocess.run(cmd, capture_output=True, text=True, timeout=3600)

        if res.returncode == 0 and os.path.exists(ruta_mp3):
            tam_mb = os.path.getsize(ruta_mp3) / 1_048_576
            log.info(f"MP3 listo: {ruta_mp3} ({tam_mb:.1f} MB)")
            return ruta_mp3

        log.error(f"yt-dlp falló: {res.stderr[:300]}")
        return None

    else:
        # ── Cámara: rtmpdump + ffmpeg ─────────────────────────────────────────
        if not _tiene_rtmpdump():
            log.error(
                "rtmpdump no está instalado. "
                "Ejecuta: sudo apt-get install -y rtmpdump"
            )
            return None

        ok = _descargar_rtmp_a_mp3(sesion.url_video, ruta_mp3)
        return ruta_mp3 if ok else None


# ══════════════════════════════════════════════════════════════════════════════
# FIRESTORE
# ══════════════════════════════════════════════════════════════════════════════
def ya_procesada(db: firestore.Client, sesion: Sesion) -> bool:
    doc = db.collection(FIRESTORE_COLECCION).document(sesion.id).get()
    if not doc.exists:
        return False
    # Si falló antes, reintentamos
    estado = doc.to_dict().get("estado", "")
    return estado not in ("error_descarga", "error_subida_audio",
                          "error_transcripcion", "error_guardado_txt")


def actualizar_estado(db: firestore.Client, sesion: Sesion,
                      estado: str, extra: dict = None):
    datos = {
        "titulo":            sesion.titulo,
        "fuente":            sesion.fuente,
        "fecha_sesion":      sesion.fecha_str,
        "url_video":         sesion.url_video,
        "uri_audio":         sesion.uri_audio,
        "uri_txt":           sesion.uri_txt,
        "estado":            estado,
        "fecha_actualizada": datetime.now(timezone.utc),
    }
    if extra:
        datos.update(extra)
    db.collection(FIRESTORE_COLECCION).document(sesion.id).set(datos, merge=True)
    log.info(f"[Firestore] {sesion.id} → {estado}  ({sesion.titulo[:50]})")


# ══════════════════════════════════════════════════════════════════════════════
# CLOUD STORAGE
# ══════════════════════════════════════════════════════════════════════════════
def _bucket():
    return storage.Client(project=GCP_PROJECT).bucket(BUCKET_NAME)


def subir_audio(ruta_local: str, sesion: Sesion) -> bool:
    try:
        _bucket().blob(sesion.ruta_audio).upload_from_filename(
            ruta_local, content_type="audio/mpeg"
        )
        log.info(f"Audio subido → {sesion.uri_audio}")
        return True
    except Exception as e:
        log.error(f"Error subiendo audio: {e}")
        return False


def subir_txt(texto: str, sesion: Sesion) -> bool:
    try:
        encabezado = (
            f"TRANSCRIPCIÓN\n"
            f"{'=' * 60}\n"
            f"Título    : {sesion.titulo}\n"
            f"Fuente    : {sesion.fuente.upper()}\n"
            f"Fecha     : {sesion.fecha_str}\n"
            f"Video URL : {sesion.url_video}\n"
            f"Audio GCS : {sesion.uri_audio}\n"
            f"{'=' * 60}\n\n"
        )
        _bucket().blob(sesion.ruta_txt).upload_from_string(
            encabezado + texto,
            content_type="text/plain; charset=utf-8"
        )
        log.info(f"TXT guardado → {sesion.uri_txt}")
        return True
    except Exception as e:
        log.error(f"Error guardando TXT: {e}")
        return False


# ══════════════════════════════════════════════════════════════════════════════
# SPEECH-TO-TEXT
# ══════════════════════════════════════════════════════════════════════════════
def transcribir(sesion: Sesion) -> str | None:
    try:
        cliente  = speech.SpeechClient()
        config   = speech.RecognitionConfig(
            encoding=speech.RecognitionConfig.AudioEncoding.MP3,
            sample_rate_hertz=44100,
            language_code="es-CL",
            enable_automatic_punctuation=True,
            diarization_config=speech.SpeakerDiarizationConfig(
                enable_speaker_diarization=True,
                min_speaker_count=2,
                max_speaker_count=10,
            ),
            speech_contexts=[speech.SpeechContext(phrases=[
                "Senado", "Cámara", "diputado", "senador", "diputada",
                "moción", "proyecto de ley", "sesión", "votación",
                "quórum", "artículo", "inciso", "reforma constitucional",
                "presidente de la comisión", "secretaría", "sala", "comisión",
                "interpelación", "acusación constitucional",
            ])],
        )
        audio = speech.RecognitionAudio(uri=sesion.uri_audio)
        log.info(f"Iniciando Speech-to-Text: {sesion.uri_audio}")
        op    = cliente.long_running_recognize(config=config, audio=audio)
        log.info("Esperando transcripción (puede tardar varios minutos)...")
        resp  = op.result(timeout=7200)

        texto = " ".join(
            r.alternatives[0].transcript for r in resp.results
        ).strip()
        log.info(f"Transcripción lista: {len(texto):,} caracteres")
        return texto

    except Exception as e:
        log.error(f"Error en Speech-to-Text: {e}")
        return None


# ══════════════════════════════════════════════════════════════════════════════
# FLUJO PRINCIPAL
# ══════════════════════════════════════════════════════════════════════════════
def main():
    db = firestore.Client(project=GCP_PROJECT)

    todas  = scrape_senado(max_paginas=5) + scrape_camara()
    log.info(f"\nTotal sesiones detectadas: {len(todas)}")

    nuevas = 0
    for sesion in todas:
        if ya_procesada(db, sesion):
            log.debug(f"Saltando: {sesion.titulo}")
            continue

        nuevas += 1
        log.info(f"\n{'─'*60}")
        log.info(f"NUEVA  : {sesion.titulo}")
        log.info(f"Fuente : {sesion.fuente.upper()}  |  Fecha: {sesion.fecha_str}")
        log.info(f"Video  : {sesion.url_video[:80]}")

        actualizar_estado(db, sesion, "detectada")

        actualizar_estado(db, sesion, "descargando")
        ruta_mp3 = descargar_mp3(sesion)
        if not ruta_mp3:
            actualizar_estado(db, sesion, "error_descarga")
            continue

        ok = subir_audio(ruta_mp3, sesion)
        os.remove(ruta_mp3)
        if not ok:
            actualizar_estado(db, sesion, "error_subida_audio")
            continue
        actualizar_estado(db, sesion, "audio_listo")

        actualizar_estado(db, sesion, "transcribiendo")
        texto = transcribir(sesion)
        if not texto:
            actualizar_estado(db, sesion, "error_transcripcion")
            continue

        ok = subir_txt(texto, sesion)
        actualizar_estado(db, sesion,
                          "listo" if ok else "error_guardado_txt",
                          extra={"caracteres": len(texto)})

    log.info(f"\n{'='*60}")
    log.info(f"Ciclo completado — sesiones nuevas: {nuevas}")


if __name__ == "__main__":
    main()