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
  // Confirmation may be declared on the form or on the specific submit
  // button that triggered the submission.
  const submitter = event.submitter;
  const confirmSource = [event.target, submitter].find(
    (el) => el instanceof Element && el.hasAttribute('data-confirm')
  );
  if (confirmSource && !window.confirm(confirmSource.getAttribute('data-confirm'))) {
    event.preventDefault();
    return;
  }
  if (submitter) {
    submitter.disabled = true;
  }
});

// Table sorting: click a table header to sort rows by that column.
document.addEventListener('click', (event) => {
  const th = event.target.closest('th');
  if (!th || !th.closest('table')) {
    return;
  }
  const table = th.closest('table');
  const tbody = table.querySelector('tbody');
  if (!tbody) {
    return;
  }
  const columnIndex = Array.from(th.parentNode.children).indexOf(th);
  const rows = Array.from(tbody.querySelectorAll('tr'));
  const alreadySorted = th.getAttribute('data-sort-dir') === 'asc';

  rows.sort((a, b) => {
    const aText = (a.children[columnIndex]?.textContent || '').trim();
    const bText = (b.children[columnIndex]?.textContent || '').trim();
    const aNum = parseFloat(aText);
    const bNum = parseFloat(bText);
    if (!isNaN(aNum) && !isNaN(bNum)) {
      return alreadySorted ? bNum - aNum : aNum - bNum;
    }
    return alreadySorted ? bText.localeCompare(aText) : aText.localeCompare(bText);
  });

  table.querySelectorAll('th').forEach((h) => h.removeAttribute('data-sort-dir'));
  th.setAttribute('data-sort-dir', alreadySorted ? 'desc' : 'asc');
  rows.forEach((row) => tbody.appendChild(row));
});
