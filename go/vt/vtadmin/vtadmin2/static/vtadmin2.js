document.addEventListener('input', (event) => {
  if (!event.target.matches('[data-table-filter]')) {
    return;
  }
  const table = document.querySelector(event.target.dataset.tableFilter);
  if (!table) {
    return;
  }
  const needle = event.target.value.toLowerCase();
  table.querySelectorAll('tbody tr').forEach((row) => {
    row.hidden = !row.textContent.toLowerCase().includes(needle);
  });
});

document.addEventListener('submit', (event) => {
  const submitter = event.submitter;
  if (!submitter) {
    return;
  }
  submitter.disabled = true;
});
