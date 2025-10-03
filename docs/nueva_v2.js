const pieDePagina = document.querySelector("app-pie-pagina");

// 2. Comprobar si existe y luego ocultarlo
if (pieDePagina) {
  pieDePagina.style.display = "none";
  console.log("Pie de página ocultado exitosamente.");
} else {
  console.warn("Elemento <app-pie-pagina> no encontrado.");
}

const controles = document.querySelector(".controles");

// 🔹 Estilos fijos
controles.style.position = "fixed"; // 👈 ya no depende del padre
controles.style.top = "1px"; // posición inicial
controles.style.transform = "translateX(-50%)";
controles.style.left = "50%";
controles.style.width = "50%"; // ancho fijo
controles.style.height = "350px"; // alto fijo
controles.style.border = "2px solid #0078d4";
controles.style.borderRadius = "8px";
controles.style.zIndex = 10000;
controles.style.cursor = "move";
controles.style.overflow = "auto";

// -------------------
// 🚀 Hacerlo arrastrable
// -------------------
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
