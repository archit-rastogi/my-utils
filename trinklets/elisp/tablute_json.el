(require 'json)
(require 'cl-lib)

;; Define a major mode for the tabulated list
(define-derived-mode my-json-table-mode tabulated-list-mode "JSON-Table"
  "Mode to display JSON data as a table."
  (setq tabulated-list-format nil)
  (setq tabulated-list-entries nil)
  (tabulated-list-init-header))

(defun my-setup-tabulated-list (data)
  "Setup tabulated list format and entries from JSON DATA."
  (let* ((columns (mapcar #'symbol-name (mapcar #'car (elt data 0))))
         (format (mapcar (lambda (col) (list col 20 t)) columns))
         (entries (cl-loop for row across data
                           for i from 0
                           collect (list i (apply #'vector (mapcar (lambda (key) (cdr (assoc key row))) columns))))))
    (setq tabulated-list-format (vconcat format))
    (setq tabulated-list-entries entries)
    (tabulated-list-init-header)
    (tabulated-list-print t)))

(defun my-json-server-handler (proc input)
  "Handle incoming data from PROC. INPUT is the received data."
  (let ((json-array-type 'list)
        (json-object-type 'alist))
    (condition-case err
        (let ((data (json-read-from-string input)))
          (with-current-buffer (get-buffer-create "*JSON Table*")
            (my-json-table-mode)
            (my-setup-tabulated-list data)
            (switch-to-buffer (current-buffer))))
      (error (message "Error parsing JSON: %s, %s" err input)))))

(defun my-start-json-server ()
  "Start a TCP server to receive JSON data."
  (make-network-process
   :name "json-server"
   :server t
   :service 9999 ;; Change the port if needed
   :filter #'my-json-server-handler
   ;; :nowait t
   )
  (message "JSON server started on port 9999"))

;; Start the server
(my-start-json-server)
