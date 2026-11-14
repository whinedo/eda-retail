// Menú de comandos y área de salida. Los botones y la ejecución llegan en la T7.
export function CommandMenu() {
  return (
    <aside className="commands" aria-labelledby="commands-title">
      <h2 id="commands-title" className="section-label">
        Comandos
      </h2>
      <p className="commands__script">scripts/notice.sh</p>
      <h2 id="output-title" className="section-label">
        Salida
      </h2>
      <pre className="output" role="log" aria-labelledby="output-title">
        Sin ejecuciones todavía. Elige un comando.
      </pre>
    </aside>
  );
}
