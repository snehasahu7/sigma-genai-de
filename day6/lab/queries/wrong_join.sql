SELECT e.name, d.department_name
FROM employees e
JOIN departments
ON e.department_id = d.id;