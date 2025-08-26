import os
from datetime import datetime
from urllib.parse import urlparse

from dotenv import load_dotenv
from playwright.sync_api import Browser, Page, Playwright, sync_playwright

# Cargar las variables de entorno desde el archivo .env
load_dotenv()


class SerService:
    """
    Servicio para interactuar con la página del SER utilizando Playwright.
    Maneja un ciclo de vida de sesión para realizar múltiples operaciones de forma eficiente.
    """

    def __init__(self):
        """
        Inicializa el servicio y las variables de estado.
        """
        self.ser_url = os.getenv("SER_URL")
        self.ser_auth_cookie = os.getenv("SER_AUTH_COOKIE")
        self.ser_url_consumo_fur = os.getenv("SER_URL_CONSUL_FUR")

        if not self.ser_url or not self.ser_auth_cookie:
            raise ValueError(
                "Las variables de entorno SER_URL y SER_AUTH_COOKIE deben estar definidas."
            )

        parsed_url = urlparse(self.ser_url)
        self.cookie_domain = parsed_url.hostname
        if not self.cookie_domain:
            raise ValueError("No se pudo extraer el dominio de la SER_URL.")

        # Atributos para gestionar el estado de Playwright durante la sesión
        self.playwright: Playwright | None = None
        self.browser: Browser | None = None
        self.page: Page | None = None

    def start_session(self):
        """
        Inicia Playwright, lanza un navegador, se autentica con la cookie
        y deja la sesión lista para ser usada en múltiples operaciones.
        Este método reemplaza al antiguo 'login'.
        """
        print("Iniciando sesión en el SER...")
        self.playwright = sync_playwright().start()
        # Cambia a headless=False si quieres ver el navegador mientras depuras
        self.browser = self.playwright.chromium.launch(headless=False)

        context = self.browser.new_context()

        # Inyectamos la cookie de autenticación
        context.add_cookies(
            [
                {
                    "name": "authCookie",
                    "value": self.ser_auth_cookie,  # type: ignore
                    "domain": self.cookie_domain,
                    "path": "/",
                }
            ]
        )

        self.page = context.new_page()
        print(f"Navegando a: {self.ser_url}")
        self.page.goto(self.ser_url, wait_until="networkidle")  # type: ignore

        # Verificamos si el login fue exitoso
        if "Account/LogOn" in self.page.url:
            raise PermissionError(
                "La cookie de autenticación es inválida o ha expirado. "
                "Actualiza SER_AUTH_COOKIE en tu archivo .env."
            )

        print("¡Sesión iniciada con éxito!")
        print(f"Título de la página: '{self.page.title()}'")

    def buscar_data(
        self, nitOperador: str, fechaInicial: datetime, fechaFinal: datetime
    ):
        """
        Con una sesión ya iniciada, busca los datos llenando el formulario y haciendo clic.
        """
        self.page.goto(self.ser_url_consumo_fur, wait_until="networkidle")  # type: ignore
        if not self.page:
            raise ConnectionError(
                "La sesión no ha sido iniciada. Llama a start_session() primero."
            )

        try:
            # Formateamos las fechas al formato que el formulario web espera (dd/mm/yyyy)
            fecha_ini_str = fechaInicial.strftime("%d/%m/%Y")
            fecha_fin_str = fechaFinal.strftime("%d/%m/%Y")

            print(
                f"Buscando datos para NIT: {nitOperador}, Periodo: {fecha_ini_str} a {fecha_fin_str}..."
            )

            # Limpiamos y luego escribimos en el campo NIT
            self.page.locator("#nitoperador").clear()
            self.page.locator("#nitoperador").type(str(nitOperador), delay=150)
            self.page.wait_for_timeout(500)

            # Limpiamos y luego escribimos en el campo de fecha inicial
            self.page.locator("#fechainicial").clear()
            self.page.locator("#fechainicial").type(fecha_ini_str, delay=150)

            # Limpiamos y luego escribimos en el campo de fecha final
            self.page.locator("#fechafinal").clear()
            self.page.locator("#fechafinal").type(fecha_fin_str, delay=150)

            print("Haciendo clic en el botón de búsqueda '#divbusqueda_xhs1d'...")
            self.page.locator("#link_aj5yn_xhs1d0").click()
            self.page.wait_for_timeout(5000)  # Espera 5 segundos

        except Exception as e:
            print(f"Error durante la búsqueda de datos para NIT {nitOperador}: {e}")
            # Opcional: tomar una captura de pantalla para depurar el error
            # self.page.screenshot(path=f"error_screenshot_{nitOperador}.png")

    def close_session(self):
        """
        Cierra el navegador y detiene la instancia de Playwright para liberar recursos.
        """
        if self.browser:
            self.browser.close()
            print("Navegador cerrado.")
        if self.playwright:
            self.playwright.stop()
            print("Sesión de Playwright finalizada.")
