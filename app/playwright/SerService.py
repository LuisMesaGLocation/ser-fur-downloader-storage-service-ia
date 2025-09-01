import os
from datetime import date
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
        self.ser_user = os.getenv("SER_USER")
        self.ser_password = os.getenv("SER_PASSWORD")
        self.ser_auth_cookie = os.getenv("SER_AUTH_COOKIE")
        self.ser_url_consumo_fur = os.getenv("SER_URL_CONSUL_FUR")
        self.download_path = os.getenv("DOWNLOAD_PATH", "descargas")

        if not self.ser_url or not self.ser_auth_cookie:
            raise ValueError(
                "Las variables de entorno SER_URL y SER_AUTH_COOKIE deben estar definidas."
            )

        if not self.ser_url or not self.ser_auth_cookie or not self.ser_url_consumo_fur:
            raise ValueError(
                "Las variables de entorno SER_URL, SER_AUTH_COOKIE y SER_URL_CONSUL_FUR deben estar definidas."
            )
        parsed_url = urlparse(self.ser_url)
        self.cookie_domain = parsed_url.hostname
        if not self.cookie_domain:
            raise ValueError("No se pudo extraer el dominio de la SER_URL.")

        # Atributos para gestionar el estado de Playwright durante la sesión
        self.playwright: Playwright | None = None
        self.browser: Browser | None = None
        self.page: Page | None = None

    def login(self):
        """
        Inicia sesión en el portal del SER usando las credenciales.
        Este método mantiene la sesión abierta para uso posterior.
        """
        print("Iniciando sesión en el SER con credenciales...")

        # Iniciamos Playwright y mantenemos la sesión abierta
        self.playwright = sync_playwright().start()
        # Lanzamos el navegador en modo "headed" (no oculto) para poder ver la interfaz
        self.browser = self.playwright.chromium.launch(headless=True)

        context = self.browser.new_context(
            viewport={"width": 1600, "height": 900}, accept_downloads=True
        )

        self.page = context.new_page()

        print(f"Navegando a la página de login: {self.ser_url}Account/LogOn")
        self.page.goto(f"{self.ser_url}Account/LogOn")

        print("Llenando formulario de login...")

        # Llenamos los campos de usuario y contraseña
        self.page.locator("#username").fill(self.ser_user)  # type: ignore
        self.page.locator("#password").fill(self.ser_password)  # type: ignore

        # tiempo de espera
        self.page.wait_for_timeout(2000)

        print("Saltándose la validación del CAPTCHA...")
        # Modificar la función ValidadCaptcha para que siempre retorne true
        self.page.evaluate("window.ValidadCaptcha = function() { return true; };")

        # Ahora hacemos clic en el botón de ingresar
        print("Haciendo clic en el botón Ingresar...")
        self.page.locator('a.jqDefaultActionButton:has-text("Ingresar")').click()

        self.page.wait_for_timeout(500)

        # Verificamos si el login fue exitoso
        if "Account/LogOn" in self.page.url:
            raise PermissionError("Las credenciales son inválidas o el login falló.")

        print("¡Sesión iniciada con éxito!")
        print(f"Título de la página: '{self.page.title()}'")

    def start_session(self, token_ser: str):
        """
        Inicia Playwright, lanza un navegador, se autentica con la cookie
        y deja la sesión lista para ser usada en múltiples operaciones.
        Este método reemplaza al antiguo 'login'.
        """
        print("Iniciando sesión en el SER...")
        self.playwright = sync_playwright().start()
        # Cambia a headless=False si quieres ver el navegador mientras depuras
        self.browser = self.playwright.chromium.launch(headless=True)

        context = self.browser.new_context(
            viewport={"width": 1600, "height": 900}, accept_downloads=True
        )

        # Inyectamos la cookie de autenticación
        context.add_cookies(
            [
                {
                    "name": "authCookie",
                    "value": token_ser,  # type: ignore
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
        self, nitOperador: str, expediente: str, fechaInicial: date, fechaFinal: date
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

            self.page.locator("#codigoexpediente").clear()
            self.page.locator("#codigoexpediente").type(str(expediente), delay=150)
            # Limpiamos y luego escribimos en el campo de fecha inicial
            self.page.locator("#fechainicial").clear()
            self.page.locator("#fechainicial").type(fecha_ini_str, delay=150)

            # Limpiamos y luego escribimos en el campo de fecha final
            self.page.locator("#fechafinal").clear()
            self.page.locator("#fechafinal").type(fecha_fin_str, delay=150)

            print("Haciendo clic en el botón de búsqueda '#divbusqueda_xhs1d'...")
            self.page.locator("#link_aj5yn_xhs1d0").click()
            self.page.wait_for_timeout(2000)  # Espera 5 segundos

        except Exception as e:
            print(f"Error durante la búsqueda de datos para NIT {nitOperador}: {e}")
            # Opcional: tomar una captura de pantalla para depurar el error
            # self.page.screenshot(path=f"error_screenshot_{nitOperador}.png")
            # ser-furs-downloader-storage-service/app/playwright/SerService.py

    def descargar_pdfs_de_tabla(
        self, nit: str, anio: int, trimestre: int, expediente: int, seecion: str
    ):
        """
        Busca en la tabla los iconos de PDF, hace clic en ellos y guarda la descarga con su nombre original.
        """
        if not self.page:
            print("Error: La página no está disponible.")
            return

        print(f"--- Iniciando descarga de PDFs para NIT {nit}, {anio}-Q{trimestre} ---")
        base_trimestre_path = os.path.join(
            self.download_path,
            seecion,
            str(anio),
            f"{nit}-{expediente}",
            f"{trimestre}T",
        )
        os.makedirs(base_trimestre_path, exist_ok=True)

        autoliquidacion_path = os.path.join(base_trimestre_path, "autoliquidacion")
        os.makedirs(autoliquidacion_path, exist_ok=True)

        try:
            scroll_container_1 = self.page.locator("#tabs-1 .scrollBar")

            # SOLUCIÓN: Verificamos si la tabla de resultados existe antes de interactuar con ella.
            if scroll_container_1.is_visible():
                print("  -> Ajustando vista y tomando captura de Autoliquidación...")
                # Localiza la imagen usando su atributo 'src' y haz clic en ella

                target_header_1 = self.page.locator('#tabs-1 th:has-text("Estado FUR")')
                scroll_container_1.evaluate(
                    "node => node.scrollLeft = (node.scrollWidth - node.clientWidth) / 1.8"
                )
                self.page.locator("#divbusqueda_xhs1d").click()
                css_personalizado = """
                            #divbusqueda_xhs1d {
                                top: 0px !important;
                                left: 900px !important;
                            }
                        """
                self.page.add_style_tag(content=css_personalizado)

                target_header_1.wait_for(state="visible", timeout=5000)
                # self.page.pause()

                self.page.wait_for_timeout(3000)
                # Imagen normal (autoliquidacion)
                screenshot_name = f"{nit}-autoliquidaciones.png"
                screenshot_path_autoliquidacion = os.path.join(
                    autoliquidacion_path, screenshot_name
                )
                screenshot_path_periodo = os.path.join(
                    base_trimestre_path, screenshot_name
                )
                self.page.screenshot(
                    path=screenshot_path_autoliquidacion, full_page=True
                )
                self.page.screenshot(path=screenshot_path_periodo, full_page=True)

                self.page.wait_for_timeout(3000)

                rows = self.page.locator("table.scrollBarProcesada tbody tr")
                num_rows = rows.count()
                print(f"Se encontraron {num_rows} filas en la tabla.")

                if num_rows == 0:
                    print("No hay datos en la tabla para descargar.")
                    return

                for i in range(num_rows):
                    row = rows.nth(i)
                    pdf_icon = row.locator("a.jqNodivLoadingForm.fa.fa-file-pdf-o")

                    if pdf_icon.count() > 0:
                        print(
                            f"  -> Haciendo clic para descargar el PDF de la fila {i + 1}..."
                        )

                        # --- LÓGICA ORIGINAL RESTAURADA ---
                        with self.page.expect_download(timeout=60000) as download_info:
                            pdf_icon.click()

                        download = download_info.value

                        # Opcional: Verificar si la descarga falló
                        failure_reason = download.failure()
                        if failure_reason:
                            print(
                                f"  -> ERROR: La descarga falló. Razón: {failure_reason}"
                            )
                            continue

                        # Usamos el nombre de archivo sugerido por el servidor
                        file_name = download.suggested_filename
                        save_path = os.path.join(autoliquidacion_path, file_name)

                        download.save_as(save_path)
                        save_path_autoliquidacion = os.path.join(
                            autoliquidacion_path, file_name
                        )
                        save_path_periodo = os.path.join(base_trimestre_path, file_name)

                        download.save_as(save_path_autoliquidacion)
                        download.save_as(save_path_periodo)
                        print(f"  -> ¡Éxito! Guardado en: {save_path}")

                        # --- FIN DE LA LÓGICA RESTAURADA ---

        except Exception as e:
            print(f"Ocurrió un error al procesar las descargas para NIT {nit}: {e}")
            if self.page and not self.page.is_closed():
                # --- INICIO DE LA MODIFICACIÓN ---
                # Construimos la ruta para la captura DENTRO de la carpeta de descarga
                screenshot_path = os.path.join(
                    autoliquidacion_path, f"error_descarga_{nit}_{trimestre}.png"
                )
                self.page.screenshot(path=screenshot_path)
                print(
                    f"  -> ¡Error! Se guardó una captura de pantalla en: {screenshot_path}"
                )

        # Aca debe ingresar a la seccion de obligaciones
        obligacion_path = os.path.join(base_trimestre_path, "obligacion")
        os.makedirs(obligacion_path, exist_ok=True)

        autoliquidacion_path = os.path.join(base_trimestre_path, "autoliquidacion")
        os.makedirs(autoliquidacion_path, exist_ok=True)

        try:
            print(
                "--- Navegando a la pestaña de Obligaciones ---:_::::__::::__::__::::__:_"
            )

            obligacion_tab_locator = self.page.locator(
                'a:has-text("FURs Generados para Obligación")'
            )

            print("  -> Pestaña 'Obligación' encontrada. Haciendo clic...")
            obligacion_tab_locator.click()
            self.page.wait_for_load_state("networkidle", timeout=1500)

            print(f"  -> Directorio de guardado para Obligaciones: {obligacion_path}")
            scroll_container_2 = self.page.locator("#tabs-2 .scrollBar")
            if scroll_container_2.is_visible():
                target_header_1 = self.page.locator('#tabs-2 th:has-text("Estado FUR")')
                scroll_container_2.evaluate(
                    "node => node.scrollLeft = (node.scrollWidth - node.clientWidth) / 1.8"
                )

                self.page.locator("#divbusqueda_xhs1d").click()
                css_personalizado = """
                            #divbusqueda_xhs1d {
                                top: 0px !important;
                                left: 900px !important;
                            }
                        """
                self.page.add_style_tag(content=css_personalizado)

                target_header_1.wait_for(state="visible", timeout=5000)
                # self.page.pause()

                self.page.wait_for_timeout(3000)
                # Imagen normal (obligacion)
                screenshot_name = f"{nit}-obligaciones.png"
                screenshot_path_obligacion = os.path.join(
                    obligacion_path, screenshot_name
                )
                screenshot_path_periodo = os.path.join(
                    base_trimestre_path, screenshot_name
                )

                print(f"  -> Captura guardada en: {screenshot_path_obligacion}")

                self.page.screenshot(path=screenshot_path_obligacion, full_page=True)
                self.page.screenshot(path=screenshot_path_periodo, full_page=True)

                self.page.wait_for_timeout(3000)

                # SOLUCIÓN: Usamos un selector específico para la tabla de obligaciones.
                rows_obligacion = self.page.locator(
                    "#tabs-2 table.scrollBarProcesada tbody tr"
                )
                num_rows_obligacion = rows_obligacion.count()

                print(
                    f"Se encontraron {num_rows_obligacion} filas en la tabla de Obligación."
                )

                for i in range(num_rows_obligacion):
                    row = rows_obligacion.nth(i)
                    pdf_icon = row.locator("a.jqNodivLoadingForm.fa.fa-file-pdf-o")

                    if pdf_icon.count() > 0:
                        print(f"  -> Descargando PDF de la fila {i + 1}...")

                        with self.page.expect_download(timeout=6000) as download_info:
                            pdf_icon.scroll_into_view_if_needed()
                            pdf_icon.click()

                        download = download_info.value
                        if download.failure():
                            print(
                                f"  -> ERROR: Descarga falló. Razón: {download.failure()}"
                            )
                            continue
                        self.page.wait_for_timeout(2000)
                        save_path = os.path.join(
                            obligacion_path, download.suggested_filename
                        )
                        download.save_as(save_path)
                        save_path_obligacion = os.path.join(
                            obligacion_path, download.suggested_filename
                        )
                        save_path_periodo = os.path.join(
                            base_trimestre_path, download.suggested_filename
                        )

                        download.save_as(save_path_obligacion)
                        download.save_as(save_path_periodo)
                        print(f"  -> ¡Éxito! Guardado en: {save_path}")

        except Exception as e:
            print(f"Ocurrió un error en la sección de Obligaciones para NIT {nit}: {e}")
            screenshot_path = os.path.join(obligacion_path, "error_obligaciones.png")
            self.page.screenshot(path=screenshot_path)
            print(
                f"  -> ¡Error! Se guardó una captura de pantalla en: {screenshot_path}"
            )

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
