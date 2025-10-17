import os
import shutil
from datetime import date, datetime
from typing import Set
from urllib.parse import urlparse

from dotenv import load_dotenv
from playwright.sync_api import Browser, Page, Playwright, sync_playwright
from typing_extensions import List

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
            viewport={"width": 1920, "height": 1080},
            device_scale_factor=2,
            accept_downloads=True,
        )

        self.page = context.new_page()

        print(f"Navegando a la página de login: {self.ser_url}")
        self.page.goto(f"{self.ser_url}")

        print("Llenando formulario de login...")

        # Llenamos los campos de usuario y contraseña
        self.page.locator("#Usuario").fill(self.ser_user)  # type: ignore
        self.page.locator("#Clave").fill(self.ser_password)  # type: ignore

        # tiempo de espera
        self.page.wait_for_timeout(15000)

        print("Saltándose la validación del CAPTCHA...")
        # Modificar la función ValidadCaptcha para que siempre retorne true
        self.page.evaluate("window.ValidadCaptcha = function() { return true; };")

        # Ahora hacemos clic en el botón de ingresar
        print("Haciendo clic en el botón Ingresar...")
        self.page.locator("#aceptar").click()

        try:
            # Espera hasta 30 segundos a que la URL contenga "/principal/index"
            print("Esperando la redirección después del login...")
            self.page.wait_for_url("**/principal/index**", timeout=15000)

            # Si la línea anterior tiene éxito, significa que el login fue correcto
            print("¡Sesión iniciada con éxito!")
            print(f"Título de la página: '{self.page.title()}'")

        except Exception as e:
            # Si después de 30 segundos la URL no es la correcta, se lanza un error
            # y se ejecuta este bloque.
            print(f"La página no redirigió a la URL esperada. Error: {e}")
            raise PermissionError("Las credenciales son inválidas o el login falló.")
        # --- FIN DEL REEMPLAZO ---

    def start_session(self, token_ser: str):
        """
        Inicia Playwright, lanza un navegador y se autentica inyectando
        el token en el localStorage.
        """
        print("Iniciando sesión en el SER con token de localStorage...")
        self.playwright = sync_playwright().start()
        # Cambia a headless=False si quieres ver el navegador mientras depuras
        self.browser = self.playwright.chromium.launch(headless=True)

        # Contexto con viewport de alta resolución para capturas de mejor calidad
        context = self.browser.new_context(
            viewport={"width": 1920, "height": 1080},
            device_scale_factor=2,
            accept_downloads=True,
        )
        self.page = context.new_page()

        # 1. Navegar a la página base para establecer el origen del localStorage
        print(f"Navegando a la URL base: {self.ser_url}")
        self.page.goto(self.ser_url, wait_until="domcontentloaded")  # type: ignore

        # 2. Inyectar el token en el localStorage del navegador
        print("Inyectando 'auth-token' en el localStorage...")
        self.page.evaluate(
            "(token) => { localStorage.setItem('auth-token', token); }",
            token_ser,
        )

        # 3. Navegar a la página de consulta final para que lea el token
        print(f"Navegando a la página de consulta: {self.ser_url_consumo_fur}")
        self.page.goto(self.ser_url_consumo_fur, wait_until="networkidle")  # type: ignore

        # 4. Verificar si el login fue exitoso esperando por un elemento clave post-login
        try:
            # Esperamos por el dropdown del operador, que es un elemento clave de la UI.
            self.page.wait_for_selector("p-dropdown", timeout=15000)
            print("¡Sesión iniciada con éxito! Elemento post-login encontrado.")
            print(f"Título de la página: '{self.page.title()}'")
        except Exception:
            # Si el elemento no aparece, la autenticación falló.
            print(
                "Error: No se pudo verificar la sesión. El token puede ser inválido o ha expirado."
            )
            self.page.screenshot(path="error_auth_storage.png")
            raise PermissionError(
                "El token de autenticación es inválido o ha expirado. "
                "No se pudo encontrar el contenido esperado después del login."
            )

    def buscar_data(
        self, nitOperador: str, expediente: str, fechaInicial: date, fechaFinal: date
    ):
        """
        Con una sesión ya iniciada, se buscan los datos llenando el formulario y haciendo clic.
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
            self.page.locator("p-dropdown").first.click()
            search_input = self.page.locator("input.p-dropdown-filter")
            search_input.fill(str(nitOperador))
            option_to_select = self.page.locator(
                f"li[role='option']:has-text('{nitOperador}')"
            )
            option_to_select.wait_for(state="visible", timeout=15000)
            option_to_select.click()

            expediente_input = self.page.locator(
                'input[formcontrolname="numeroExpediente"]'
            )
            expediente_input.clear()
            expediente_input.type(str(expediente), delay=150)

            # --- CAMPO FECHA INICIAL ---
            fecha_inicio_input = self.page.locator(
                'p-calendar[formcontrolname="fechaInicio"] input'
            )
            print(f"  -> Escribiendo la fecha inicial: {fecha_ini_str}...")
            fecha_inicio_input.click()  # Hacemos clic para asegurar que el campo tiene foco
            fecha_inicio_input.clear()
            # Escribimos la fecha lentamente para simular un humano
            fecha_inicio_input.type(fecha_ini_str, delay=100)

            # --- CAMPO FECHA FINAL ---
            fecha_fin_input = self.page.locator(
                'p-calendar[formcontrolname="fechaFin"] input'
            )
            print(f"  -> Escribiendo la fecha final: {fecha_fin_str}...")
            fecha_fin_input.click()  # Hacemos clic para asegurar que el campo tiene foco
            fecha_fin_input.clear()
            # Escribimos la fecha lentamente para simular un humano
            fecha_fin_input.type(fecha_fin_str, delay=100)

            print("Haciendo clic en el botón 'Consultar'...")
            consultar_button = self.page.locator("button:has-text('Consultar')")
            consultar_button.click()

            # 1. Guarda tu script en una variable
            javascript_code = """
            () => { // Se envuelve en una función para asegurar la correcta ejecución
                const pieDePagina = document.querySelector("app-pie-pagina");

                // 2. Comprobar si existe y luego ocultarlo
                if (pieDePagina) {
                pieDePagina.style.display = "none";
                console.log("Pie de página ocultado exitosamente.");
                } else {
                console.warn("Elemento <app-pie-pagina> no encontrado.");
                }

                const controles = document.querySelector(".controles");
                if (!controles) {
                    console.error('Elemento ".controles" no encontrado.');
                    return;
                }

                // Estilos fijos
                controles.style.position = "fixed";
                controles.style.top = "0px"; // posición inicial
                controles.style.transform = "translateX(-50%)";
                controles.style.left = "50%";
                controles.style.width = "50%";
                controles.style.height = "350px";
                controles.style.border = "2px solid #0078d4";
                controles.style.borderRadius = "8px";
                controles.style.zIndex = 10000;
                controles.style.cursor = "move";
                controles.style.overflow = "auto";

                // Hacerlo arrastrable
                let isDragging = false;
                let offsetX = 0;
                let offsetY = 0;

                controles.addEventListener("mousedown", (e) => {
                  isDragging = true;
                  offsetX = e.clientX - controles.offsetLeft;
                  offsetY = e.clientY - controles.offsetTop;
                  controles.style.userSelect = "none";
                });

                document.addEventListener("mousemove", (e) => {
                  if (isDragging) {
                    controles.style.left = e.clientX - offsetX + "px";
                    controles.style.top = e.clientY - offsetY + "px";
                  }
                });

                document.addEventListener("mouseup", () => {
                  isDragging = false;
                  controles.style.userSelect = "auto";
                });
            }
            """

            # 2. Ejecuta el script en el navegador
            self.page.evaluate(javascript_code)
            self.page.wait_for_timeout(3000)

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

    def descargar_y_clasificar_furs_paginado(
        self, nit: str, anio: int, expediente: int, seccion: str, trimestres: List[int]
    ):
        """
        Navega a través de la paginación, toma capturas de pantalla de filas colapsadas y expandidas,
        y descarga todos los PDFs, clasificándolos en carpetas por año y trimestre.
        """
        if not self.page or self.page.is_closed():
            print("Error: La página no está disponible o ha sido cerrada.")
            return

        print(
            f"--- Iniciando descarga y clasificación para NIT {nit}, año de búsqueda {anio} ---"
        )

        # --- FASE 1: PREPARACIÓN Y CAPTURA DE PANTALLAS ---
        created_period_paths: Set[str] = set()
        base_search_year_path = os.path.join(
            self.download_path, seccion, str(anio), f"{nit}-{expediente}"
        )
        os.makedirs(base_search_year_path, exist_ok=True)

        # Usaremos listas para guardar las rutas de las capturas de cada página
        screenshot_colapsada_paths: List[str] = []
        screenshot_expandida_paths: List[str] = []

        # Ocultar el pie de página para que no interfiera con las capturas
        self.page.evaluate(
            '() => { const pf = document.querySelector("app-pie-pagina"); if (pf) pf.style.display = "none"; }'
        )

        page_num = 1
        while True:
            print(f"\n--- Procesando página {page_num} ---")

            # Esperar a que la tabla se cargue y esté estable
            self.page.wait_for_selector("div.p-datatable-wrapper", timeout=20000)
            self.page.wait_for_timeout(2000)  # Tiempo extra para renderizado

            # --- SCRIPT PARA MOSTRAR FILTROS ---

            script_mostrar_y_posicionar_filtros = """
            () => {
                const resultados = document.querySelector(".resultados");
                const filtros = document.querySelector(".controles");

                if (filtros && resultados) {
                    // --- CAMBIOS CLAVE ---
                    filtros.style.display = "block";
                    filtros.style.position = "relative";
                    filtros.style.margin = "20px auto";
                    filtros.style.width = "50%";
                    resultados.style.marginTop = "20px";
                    filtros.style.zIndex = "10000";
                    filtros.style.background = "white";
                }

                const pieDePagina = document.querySelector("app-pie-pagina");
                if (pieDePagina) pieDePagina.style.display = "none";
            }
            """

            self.page.evaluate(script_mostrar_y_posicionar_filtros)
            print(
                "  -> Filtros posicionados en la parte superior para la captura 'colapsada'."
            )

            # --- Captura Colapsada ---
            screenshot_colapsada_path = os.path.join(
                base_search_year_path, f"{nit}-colapsada-pag-{page_num}.png"
            )
            self.page.screenshot(path=screenshot_colapsada_path, full_page=True)
            screenshot_colapsada_paths.append(screenshot_colapsada_path)
            print(f"  -> Captura 'colapsada' guardada en: {screenshot_colapsada_path}")

            # --- SOLUCIÓN: OCULTAR ELEMENTOS MOLESTOS ANTES DE LA CAPTURA ---
            script_ocultar_elementos = """
            () => {
                const pieDePagina = document.querySelector("app-pie-pagina");
                if (pieDePagina) pieDePagina.style.display = "none";

                const filtros = document.querySelector(".controles");
                if (filtros) filtros.style.display = "none";
            }
            """
            self.page.evaluate(script_ocultar_elementos)
            self.page.evaluate(script_ocultar_elementos)
            self.page.evaluate(script_ocultar_elementos)
            print(
                "  -> Elementos de la UI (filtros, pie de página) ocultados para la captura."
            )

            # --- Expandir todas las filas ---
            expand_buttons = self.page.locator("button.boton-expandir")
            if expand_buttons.count() > 0:
                print(f"  -> Expandiendo {expand_buttons.count()} filas...")
                for i in range(expand_buttons.count()):
                    expand_buttons.nth(i).click()
                    self.page.wait_for_timeout(200)  # Pequeña pausa entre clics

                self.page.wait_for_timeout(
                    3000
                )  # Esperar a que todo el contenido se cargue

                # --- Captura Expandida ---
                screenshot_expandida_path = os.path.join(
                    base_search_year_path, f"{nit}-expandida-pag-{page_num}.png"
                )

                self.page.screenshot(path=screenshot_expandida_path, full_page=True)
                screenshot_expandida_paths.append(screenshot_expandida_path)
                print(
                    f"  -> Captura 'expandida' guardada en: {screenshot_expandida_path}"
                )
            else:
                print("  -> No se encontraron filas para expandir en esta página.")

            # --- FASE 2: PROCESAR FILAS Y DESCARGAR PDFS ---
            rows = self.page.locator("tbody.p-datatable-tbody > tr")
            print(f"  -> Procesando {rows.count()} filas en la página {page_num}...")

            for i in range(rows.count()):
                row = rows.nth(i)
                # Omitir filas de detalle (las expandidas) en el bucle principal
                if "p-datatable-row-expansion" in (row.get_attribute("class") or ""):
                    continue

                try:
                    # --- VALIDACIÓN DE ESTADO FUR ---
                    # La columna "Estado FUR" es la 7ma (índice 6).
                    estado_fur_str = (
                        row.locator("td")
                        .nth(6)
                        .inner_text(timeout=5000)
                        .strip()
                        .lower()
                    )

                    if estado_fur_str in ["vencido", "anulado"]:
                        print(
                            f"     -> Fila {i + 1}: Omitiendo, estado es '{estado_fur_str.capitalize()}'."
                        )
                        continue  # Salta al siguiente registro
                    # La columna de fecha inicial es la 4ta (índice 3)
                    fecha_inicial_str = (
                        row.locator("td").nth(3).inner_text(timeout=5000)
                    )
                    fecha_obj = datetime.strptime(
                        fecha_inicial_str.strip(), "%d/%m/%Y"
                    ).date()

                    anio_real = fecha_obj.year
                    trimestre = (fecha_obj.month - 1) // 3 + 1

                    period_path = os.path.join(
                        self.download_path,
                        seccion,
                        str(anio_real),
                        f"{nit}-{expediente}",
                        f"{trimestre}T",
                    )
                    os.makedirs(period_path, exist_ok=True)
                    created_period_paths.add(period_path)

                    # El ícono de PDF/acción está en la última columna
                    pdf_icon = row.locator("td:last-child div.ver-fur")
                    if pdf_icon.count() > 0:
                        with self.page.expect_download(timeout=60000) as dl_info:
                            pdf_icon.click()

                        download = dl_info.value
                        original_filename = download.suggested_filename

                        # --- INICIO DE LA MODIFICACIÓN ---
                        # Separa el nombre del archivo de su extensión
                        name_part, extension = os.path.splitext(original_filename)

                        # Si hay un guion bajo en el nombre...
                        if "_" in name_part:
                            # ...nos quedamos solo con la parte antes del primer guion bajo
                            base_name = name_part.split("_")[0]
                            # Creamos el nuevo nombre de archivo
                            new_filename = f"{base_name}{extension}"
                        else:
                            # Si no hay guion bajo, usamos el nombre original
                            new_filename = original_filename
                        # --- FIN DE LA MODIFICACIÓN ---

                        # Usamos el nuevo nombre de archivo para guardarlo
                        save_path = os.path.join(period_path, new_filename)
                        download.save_as(save_path)

                        print(
                            f"     -> Fila {i + 1}: PDF del {anio_real}-T{trimestre} guardado en {save_path}."
                        )

                    else:
                        print(
                            f"     -> Fila {i + 1}: No se encontró ícono de descarga."
                        )

                except Exception as e:
                    print(f"     -> ERROR procesando fila {i + 1}: {e}")

            # --- FASE 3: NAVEGAR A LA SIGUIENTE PÁGINA ---
            next_button = self.page.locator("button.p-paginator-next")
            if next_button.count() == 0 or next_button.is_disabled():
                print(
                    "--- Fin de la paginación. Es la última página o no hay paginador. ---"
                )
                break

            print("  -> Navegando a la siguiente página...")
            next_button.click()
            page_num += 1

        # --- FASE 4: VERIFICAR TRIMESTRES FALTANTES ---
        print(
            "\n--- Verificando y creando carpetas para trimestres sin datos encontrados ---"
        )
        for trimestre in trimestres:
            expected_period_path = os.path.join(base_search_year_path, f"{trimestre}T")
            if expected_period_path not in created_period_paths:
                print(
                    f"  -> No se encontraron datos para T{trimestre}. Creando directorio de evidencia."
                )
                os.makedirs(expected_period_path, exist_ok=True)
                created_period_paths.add(expected_period_path)

        # --- FASE 5: COPIA DINÁMICA DE IMÁGENES DE EVIDENCIA ---
        print("\n--- Iniciando copia dinámica de imágenes de evidencia ---")
        all_screenshots = screenshot_colapsada_paths + screenshot_expandida_paths
        for period_path in created_period_paths:
            for img_path in all_screenshots:
                if os.path.exists(img_path):
                    try:
                        shutil.copy(img_path, period_path)
                    except Exception as e:
                        print(
                            f"  -> ERROR al copiar imagen {os.path.basename(img_path)} a {period_path}: {e}"
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
